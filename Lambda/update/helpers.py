import xmltodict
import time
import uuid
import csv
import boto3
import os


def file_exists_s3(bucket_name, object_key):
    s3 = boto3.client('s3')
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
        print(e)
        raise e


def label_files_to_csv(dest_bucket,
                       dest_path_key_lbl,
                       dest_path_key_anno,
                       src_full_file_path,
                       xml_data,
                       product,
                       operation,
                       op_year):

    TEMP_LBL_PATH = '/tmp/output-lbls.csv'
    TEMP_ANNO_PATH = '/tmp/output-annos.csv'

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
        # "id": str(uuid.uuid4()),
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
        "year": str(op_year)
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
                "occluded": lb_class["occluded"],
                "truncated": lb_class["truncated"],
                "obstacle": is_obstical,
                "difficult": lb_class["difficult"],
                "bndbox.xmin": lb_class["bndbox"]["xmin"],
                "bndbox.ymin": lb_class["bndbox"]["ymin"],
                "bndbox.xmax": lb_class["bndbox"]["xmax"],
                "bndbox.ymax": lb_class["bndbox"]["ymax"]
            }
            lbls.append(lbl)

    with open(TEMP_ANNO_PATH, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(anno.keys())
        writer.writerows([anno.values()])

    if len(lbls) > 0:
        with open(TEMP_LBL_PATH, 'w') as f:
            writer = csv.writer(f)
            lbl_values = [lbl.values() for lbl in lbls]
            writer.writerow(lbls[0].keys())
            writer.writerows(lbl_values)

    # Push files to s3
    print("Pushing annotation to: ", os.path.join(dest_bucket, dest_path_key_anno))
    upload_to_s3(TEMP_ANNO_PATH, dest_bucket, dest_path_key_anno)
    print("Pushing label to: ", os.path.join(dest_bucket, dest_path_key_lbl))
    upload_to_s3(TEMP_LBL_PATH, dest_bucket, dest_path_key_lbl)

    print("done")


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
            # "id": str(uuid.uuid4()),
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
