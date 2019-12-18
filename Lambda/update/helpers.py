import xmltodict
import time
import uuid
import datetime
now = datetime.datetime.now()


def label_files_to_dynamo(full_file_path, xml_data, tbl_anno, tbl_lbl):
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
            "year": str(now.year)
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
        print("anno['image_name']:", anno['image_name'])
