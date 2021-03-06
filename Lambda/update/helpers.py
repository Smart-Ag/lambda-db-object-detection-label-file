import xmltodict
import time
import uuid
import csv
import boto3
import os


def create_db_entry(op_year, src_bucket, src_path_key, og_filename, s3):
    dest_path_key_lbl, dest_path_key_anno = get_anno_lbl_db_path(og_filename, op_year)

    src_full_file_path = os.path.join(src_bucket, src_path_key)

    dest_bucket = src_bucket[0:]

    print("About to read bucket=", src_bucket, "key=", src_path_key)
    response = s3.Object(src_bucket, src_path_key).get()
    xml_data = response['Body'].read()

    meta_data = get_meta_data(og_filename)

    label_files_to_csv(
        dest_bucket,
        dest_path_key_lbl,
        dest_path_key_anno,
        src_full_file_path,
        xml_data,
        meta_data,
        op_year)

    return response


def get_anno_lbl_db_path(og_filename, op_year, log=True):
    dest_path_key_anno = 'anno/object_detection_label_file_' + str(op_year)
    dest_path_key_lbl = 'lbl/object_detection_label_file_item_' + str(op_year)

    dest_path_key_lbl = os.path.join(
        "database",
        dest_path_key_lbl, og_filename + '.csv')
    if log:
        print("dest_path_key_lbl:", dest_path_key_lbl)
    dest_path_key_anno = os.path.join(
        "database",
        dest_path_key_anno, og_filename + '.csv')
    if log:
        print("dest_path_key_anno:", dest_path_key_anno)
    return dest_path_key_lbl, dest_path_key_anno


def get_meta_data(file_name):
    # v2019-0-0-4e97fb4008634f6689dd3e4ab130f601
    # -nodetection-primary-forward-57-smartag-autocart-1579025339707.jpeg

    meta = {
        "version": "2019-0-0",
        "det_type": "n/a",
        "cam_func": "n/a",
        "cam_pos": "n/a",
        "cam_fov": "n/a",
        "product": "smartag",
        "op": "autocart",
        "collection_id": "n/a"
    }

    if file_name.startswith("v2020-3-0"):
        file_name_split = file_name.split('-')

        meta["version"] = file_name_split[0][1:] +\
            "-" + file_name_split[1] +\
            "-" + file_name_split[2]
        meta["collection_id"] = file_name_split[3]
        meta["det_type"] = file_name_split[4]
        meta["cam_func"] = file_name_split[5]
        meta["cam_pos"] = file_name_split[6]
        meta["cam_fov"] = file_name_split[7]
        meta["product"] = file_name_split[8]
        meta["op"] = file_name_split[9]

    return meta


def file_exists_s3(bucket_name, object_key):
    s3 = boto3.client('s3')

    return file_exists_s3_on_prem(s3, bucket_name, object_key)


def file_exists_s3_on_prem(s3, bucket_name, object_key):
    try:
        s3.head_object(Bucket=bucket_name, Key=object_key)
    except BaseException:
        # Not found
        return False

    return True


def upload_to_s3(source_file, bucket_name, object_key):
    s3 = boto3.resource('s3')

    # Uploads the source file to the specified s3 bucket by using a
    # managed uploader. The uploader automatically splits large
    # files and uploads parts in parallel for faster uploads.
    try:
        s3.Bucket(bucket_name).upload_file(source_file, object_key)
    except Exception as e:

        print('[Error]: uploading source file={0} to s3 bucket={1}, object={2}:'
              .format(
                  source_file,
                  bucket_name,
                  object_key
              ))
        print("upload_to_s3:", e)
        raise e


def label_files_to_csv(dest_bucket,
                       dest_path_key_lbl,
                       dest_path_key_anno,
                       src_full_file_path,
                       xml_data,
                       meta_data,
                       op_year):

    TEMP_LBL_PATH = '/tmp/output-lbls.csv'
    TEMP_ANNO_PATH = '/tmp/output-annos.csv'

    anno, lbls = xml_to_anno_lbls(meta_data, xml_data, src_full_file_path, op_year)

    anno_lbl_to_csv(TEMP_ANNO_PATH, anno, lbls, TEMP_LBL_PATH)

    # Push files to s3
    print("Pushing annotation to: ", os.path.join(dest_bucket, dest_path_key_anno))
    upload_to_s3(TEMP_ANNO_PATH, dest_bucket, dest_path_key_anno)
    print("Pushing label to: ", os.path.join(dest_bucket, dest_path_key_lbl))
    upload_to_s3(TEMP_LBL_PATH, dest_bucket, dest_path_key_lbl)

    print("done")


def anno_lbl_to_csv(TEMP_ANNO_PATH, anno, lbls, TEMP_LBL_PATH, append=False, append_count=0):
    file_mode = "w"
    if append:
        file_mode = "a+"

    annos_existed = os.path.exists(TEMP_ANNO_PATH)
    lbls_existed = os.path.exists(TEMP_LBL_PATH)

    with open(TEMP_ANNO_PATH, file_mode) as f:
        writer = csv.writer(f)
        if append_count == 0 or not annos_existed:
            print("Anno append count:", append_count)
            writer.writerow(anno.keys())
        writer.writerows([anno.values()])

    if len(lbls) > 0:
        with open(TEMP_LBL_PATH, file_mode) as f:
            writer = csv.writer(f)
            lbl_values = [lbl.values() for lbl in lbls]
            if append_count == 0 or not lbls_existed:
                print("lbl append count:", append_count)
                writer.writerow(lbls[0].keys())
            writer.writerows(lbl_values)


def xml_to_anno_lbls(meta_data, xml_data, src_full_file_path, op_year):
    product = meta_data["product"]
    operation = meta_data["op"]
    data_version = meta_data["version"]

    json_data = xmltodict.parse(xml_data)

    json_data["full_file_path"] = src_full_file_path
    json_data["file_name"] = src_full_file_path.split("/")[-1]
    json_data["last_updated_time"] = str(time.time())

    if not json_data["annotation"].get("object", False):
        json_data["annotation"]["object"] = []
    elif not isinstance(json_data["annotation"]["object"], list):
        json_data["annotation"]["object"] = [
            json_data["annotation"]["object"]]

    anno = {
        "app": product,
        "operation": operation,
        "image_name": str(json_data['annotation']['filename']),
        "anno_name": json_data["file_name"],
        "full_file_path": json_data["full_file_path"],
        "last_updated_time": str(json_data["last_updated_time"]),
        "size.width": json_data['annotation']['size']['width'],
        "size.height": json_data['annotation']['size']['height'],
        "size.depth": json_data['annotation']['size']['depth'],
        "segmented": "0",
        "unique": "1",
        "year": str(op_year),
        "data_version": str(data_version),
        "det_type": meta_data["det_type"],
        "cam_func": meta_data["cam_func"],
        "cam_pos": meta_data["cam_pos"],
        "cam_fov": meta_data["cam_fov"],
        "collection_id": meta_data["collection_id"]
    }

    lbls = []
    lb_classes = json_data["annotation"]["object"]
    for lb_class in lb_classes:
        if lb_class.get("bndbox", False) is not False:
            is_obstical = lb_class.get("obstacle", '0')
            lbl = {
                "id": str(uuid.uuid4()),
                "anno_name": anno["anno_name"],
                "last_updated_time": str(json_data["last_updated_time"]),
                "name": lb_class["name"],
                "pose": lb_class["pose"],
                "occluded": lb_class.get("occluded", 'n/a'),
                "truncated": lb_class["truncated"],
                "obstacle": is_obstical,
                "difficult": lb_class["difficult"],
                "bndbox.xmin": lb_class["bndbox"]["xmin"],
                "bndbox.ymin": lb_class["bndbox"]["ymin"],
                "bndbox.xmax": lb_class["bndbox"]["xmax"],
                "bndbox.ymax": lb_class["bndbox"]["ymax"]
            }
            lbls.append(lbl)

    return anno, lbls


def label_files_to_dynamo(full_file_path, xml_data, tbl_anno, tbl_lbl, op_year):
    with tbl_anno.batch_writer() as anno_batch:
        json_data = xmltodict.parse(xml_data)

        json_data["full_file_path"] = full_file_path
        json_data["file_name"] = full_file_path.split("/")[-1]
        json_data["last_updated_time"] = str(time.time())

        if not json_data["annotation"].get("object", False):
            json_data["annotation"]["object"] = []
        elif not isinstance(json_data["annotation"]["object"], list):
            json_data["annotation"]["object"] = [json_data["annotation"]["object"]]

        anno = {
            "app": "autocart",
            "image_name": str(json_data['annotation']['filename']),
            "anno_name": json_data["file_name"],
            "full_file_path": json_data["full_file_path"],
            "last_updated_time": str(json_data["last_updated_time"]),
            "size.width": json_data['annotation']['size']['width'],
            "size.height": json_data['annotation']['size']['height'],
            "size.depth": json_data['annotation']['size']['depth'],
            "segmented": "0",
            "unique": "1",
            "year": str(op_year)
        }

        lb_classes = json_data["annotation"]["object"]
        with tbl_lbl.batch_writer() as lbl_batch:
            for lb_class in lb_classes:
                if lb_class.get("bndbox", False) is not False:
                    is_obstical = lb_class.get("obstacle", '0')
                    lbl = {
                        "id": str(uuid.uuid4()),
                        "anno_name": anno["anno_name"],
                        "last_updated_time": str(json_data["last_updated_time"]),
                        "name": lb_class["name"],
                        "pose": lb_class["pose"],
                        "occluded": lb_class["occluded"],
                        "truncated": lb_class["truncated"],
                        "obstacle": is_obstical,
                        "difficult": lb_class["difficult"],
                        "bndbox.xmin": lb_class["bndbox"]["xmin"],
                        "bndbox.ymin": lb_class["bndbox"]["ymin"],
                        "bndbox.xmax": lb_class["bndbox"]["xmax"],
                        "bndbox.ymax": lb_class["bndbox"]["ymax"]
                    }

                    lbl_batch.put_item(Item=lbl)

        anno_batch.put_item(Item=anno)

        print("done")
