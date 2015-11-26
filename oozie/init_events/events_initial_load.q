-- we want compression on the output
SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET avro.output.codec=snappy;

SET landing_table=landing.raw_events;
SET bad_table=landing.events_bad;
SET good_table=staging.events;

FROM ${hiveconf:landing_table} lnd_in
-- first table captures bad rows
INSERT INTO TABLE ${hiveconf:bad_table}
  -- this DISTINCT is where we handle any duplicate rows from flume redelivery
  SELECT DISTINCT
    lnd_in.eventid,
    lnd_in.username,
    lnd_in.unix_ts,
    FROM_UNIXTIME(lnd_in.unix_ts) AS event_date
   WHERE
    lnd_in.eventid IS NULL
    OR
    -- we are saying here usernames can't be empty strings
    lnd_in.username IS NULL OR LENGTH(lnd_in.username) = 0
    OR
    -- we check this is a valid number by trying to convert it next
    lnd_in.unix_ts IS NULL
    OR
    FROM_UNIXTIME(lnd_in.unix_ts) IS NULL
INSERT INTO TABLE ${hiveconf:good_table}
  -- this DISTINCT is where we handle any duplicate rows from flume redelivery
  SELECT DISTINCT
    lnd_in.eventid,
    lnd_in.username,
    lnd_in.unix_ts,
    FROM_UNIXTIME(lnd_in.unix_ts) AS event_date
   WHERE
    lnd_in.eventid IS NOT NULL
    AND
    -- we are saying here usernames can't be empty strings
    lnd_in.username IS NOT NULL AND LENGTH(lnd_in.username) > 0
    AND
    -- we check this is a valid number by trying to convert it next
    lnd_in.unix_ts IS NOT NULL
    AND
    FROM_UNIXTIME(lnd_in.unix_ts) IS NOT NULL;

