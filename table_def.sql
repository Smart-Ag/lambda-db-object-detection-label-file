CREATE EXTERNAL TABLE `object_detection_label_file_v2`(
    `app` string,
    `operation` string,
    `image_name` string,
    `anno_name` string,
    `full_file_path` string,
    `last_updated_time` string,
    `size.width` string,
    `size.height` string,
    `size.depth` string,
    `segmented` string,
    `unique` string,
    `year` string,
    `data_version` string,
    `det_type` string,
    `cam_func` string,
    `cam_pos` string,
    `cam_fov` string,
    `collection_id` string
)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://object-detection-models-training-data/database'
TBLPROPERTIES (
  'has_encrypted_data'='false', 
  'transient_lastDdlTime'='1578332910')

CREATE EXTERNAL TABLE `object_detection_label_file_item_v2`(
  `id` string,
  `anno_name` string,
  `last_updated_time` string,
  `name` string,
  `pose` string,
  `occluded` string,
  `truncated` string,
  `obstacle` string,
  `difficult` string,
  `bndbox.xmin` string,
  `bndbox.ymin` string,
  `bndbox.xmax` string,
  `bndbox.ymax` string
)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://object-detection-models-training-data/database'
TBLPROPERTIES (
  'has_encrypted_data'='false', 
  'transient_lastDdlTime'='1578333083')
