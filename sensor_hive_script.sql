create database packt_oozie;
use packt_oozie;
create table oozie_sensor (
id INT,
attitude_roll DOUBLE,
attitude_pitch DOUBLE,
attitude_yaw DOUBLE,
gravity_x DOUBLE,
gravity_y DOUBLE,
gravity_z DOUBLE,
rotationRate_x DOUBLE,
rotationRate_y DOUBLE,
rotationRate_z DOUBLE,
userAcceleration_x DOUBLE,
userAcceleration_y DOUBLE,
userAcceleration_z DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
load data inpath 'hdfs://quickstart.cloudera:8020/packt/oozie/sensor/sensor_data.csv' into table oozie_sensor;
insert overwrite directory 'hdfs://quickstart.cloudera:8020/packt/oozie/sensor/max_attitude_pitch' select id from oozie_sensor where attitude_pitch = -0.664509;
