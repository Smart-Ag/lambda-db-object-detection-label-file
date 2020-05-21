import boto3
import sys
import os
import update.helpers as helpers


def get_folders(result, index):
    # index 0 = bucket top level
    # index 1 = year top level
    folders = []
    res_common = result.get('CommonPrefixes')
    for o in res_common:
        folders.append(str(o.get('Prefix').split('/')[index]))

    print("Found top level folders:", folders)
    return folders


def get_top_level_folders_bucket(s3, bucket, only_year=False):

    result = s3.list_objects(Bucket=bucket, Delimiter='/')

    years = get_folders(result, 0)

    if only_year:
        temp = []
        for y in years:
            if len(y) == 4:
                temp.append(y)

        return temp
    else:
        return years


def get_top_level_folders_year(s3, bucket, prefix):

    result = s3.list_objects(Bucket=bucket, Prefix=prefix + "/", Delimiter='/')

    return get_folders(result, 1)


def get_matching_s3_objects(s3, bucket, prefix="", suffix=""):
    """
    Generate objects in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch objects whose key starts with
        this prefix (optional).
    :param suffix: Only fetch objects whose keys end with
        this suffix (optional).
    """
    paginator = s3.get_paginator("list_objects_v2")

    kwargs = {'Bucket': bucket}

    # We can pass the prefix directly to the S3 API.  If the user has passed
    # a tuple or list of prefixes, we go through them one by one.
    if isinstance(prefix, str):
        prefixes = (prefix, )
    else:
        prefixes = prefix

    for key_prefix in prefixes:
        kwargs["Prefix"] = key_prefix

        for page in paginator.paginate(**kwargs):
            try:
                contents = page["Contents"]
            except KeyError:
                break

            for obj in contents:
                key = obj["Key"]
                if key.endswith(suffix):
                    yield obj


def get_matching_s3_keys(s3, bucket, prefix="", suffix=""):
    """
    Generate the keys in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (optional).
    :param suffix: Only fetch keys that end with this suffix (optional).
    """

    for obj in get_matching_s3_objects(s3, bucket, prefix, suffix):
        yield obj["Key"]


def create_anno_db_entries(bucket_name, key="", secret=""):
    boto3.setup_default_session(region_name='us-east-1')

    s3 = boto3.client(
        's3',
        aws_access_key_id=key,
        aws_secret_access_key=secret)

    s3_alt = boto3.resource(
        's3',
        aws_access_key_id=key,
        aws_secret_access_key=secret)

    num_files_missing = 0
    years = get_top_level_folders_bucket(s3, bucket_name, only_year=True)
    for year in years:
        print("Doing year:", year)
        weeks = get_top_level_folders_year(s3, bucket_name, year)
        for week in weeks:
            full_prefix = os.path.join(year, week)
            print("Doing prefix:", full_prefix)
            xmls = get_matching_s3_keys(s3, bucket_name, prefix=full_prefix)
            count = 0
            for xml_key in xmls:
                count += 1
                if count % 1000 == 0:
                    print("Finished another set for prefix=", full_prefix)
                    count = 0
                try:
                    xml_path_split = xml_key.split('/')
                    og_filename = xml_path_split[-1]

                    dest_path_key_lbl, dest_path_key_anno = helpers.get_anno_lbl_db_path(
                        og_filename,
                        year,
                        log=False)
                    db_anno_exists = helpers.file_exists_s3_on_prem(
                        s3,
                        bucket_name,
                        dest_path_key_anno)

                    if not db_anno_exists:
                        print('\n\n--------------------------------------------------')
                        num_files_missing += 1
                        print("Found missing anno xml_key={0}, db_addr={1}".format(
                            xml_key, dest_path_key_anno))
                        print("num_files_missing=", num_files_missing)

                        helpers.create_db_entry(
                            op_year=year,
                            src_bucket=bucket_name,
                            src_path_key=xml_key,
                            og_filename=og_filename,
                            s3=s3_alt)
                        print('--------------------------------------------------\n\n')
                except Exception as e:
                    print("Failed with:", e)
                    print("[xml_key failed xml_key]:", xml_key)

    return num_files_missing


def main():
    try:
        print('Number of arguments:', len(sys.argv), 'arguments.')
        print('Argument List:', str(sys.argv))
        if len(sys.argv) < 4:
            raise Exception("[Error]: no source bucket, key, secret specified.")

        bucket_name = sys.argv[1]
        key = sys.argv[2]
        secret = sys.argv[3]
        create_anno_db_entries(bucket_name, prefix="", key=key, secret=secret)
    except Exception as e:
        print("[Error]:", e)


if __name__ == "__main__":
    main()
