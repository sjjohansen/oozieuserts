-- our target staging table for good events
CREATE TABLE IF NOT EXISTS staging.events
(eventid int,
 username string,
 unix_ts bigint,
 event_date string
 )
STORED AS AVRO;

-- our target table for rows that wont parse
CREATE TABLE IF NOT EXISTS landing.events_bad
(eventid int,
 username string,
 unix_ts bigint,
 event_date string
 )
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';

