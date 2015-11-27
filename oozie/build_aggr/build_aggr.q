DROP TABLE IF EXISTS warehouse.user_counts;

CREATE TABLE warehouse.user_counts
(username string,
 num_events bigint)
 PARTITIONED BY (ymd string)
 STORED AS PARQUET;

INSERT OVERWRITE TABLE warehouse.user_counts PARTITION (ymd)
 SELECT
  username,
  COUNT(username),
  FROM_UNIXTIME(unix_ts, 'yyyy-MM-dd') AS ymd
 FROM staging.events
 GROUP BY username, ymd;

