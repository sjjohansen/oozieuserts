SET landing_table=landing.raw_events;

DROP TABLE IF EXISTS ${hiveconf:landing_table};

CREATE EXTERNAL TABLE ${hiveconf:landing_table}
(eventid int,
 username string,
 unix_ts bigint)
 ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
 STORED AS SEQUENCEFILE
 LOCATION '/user/flume/processing/${DATAFEED}/${WORKING_DIR}';

