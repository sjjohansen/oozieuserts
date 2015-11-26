-- we want compression on the output
SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET avro.output.codec=snappy;

SET landing_table=landing.raw_events;
SET bad_table=landing.events_bad;
SET good_table=staging.events;

-- we do a UNION ALL between landing and staging as our source table
-- we also add the bad rows table in case they havent been inspected yet
FROM (
  SELECT
   lnd_in.eventid,
   lnd_in.username,
   lnd_in.unix_ts,
   FROM_UNIXTIME(lnd_in.unix_ts) AS event_date
   FROM ${hiveconf:landing_table} lnd_in
  UNION ALL
   SELECT
    stg_in.eventid,
    stg_in.username,
    stg_in.unix_ts,
    stg_in.event_date
    FROM ${hiveconf:good_table} stg_in
  UNION ALL
   SELECT
    bad_in.eventid,
    bad_in.username,
    bad_in.unix_ts,
    bad_in.event_date
   FROM ${hiveconf:bad_table} bad_in
  ) inputs_union
-- first insert statement to handle bad rows
-- note we overwrite the table here and distinct
-- our assumption is there are no bad rows in staging
INSERT OVERWRITE TABLE ${hiveconf:bad_table}
 SELECT DISTINCT
  inputs_union.eventid,
  inputs_union.username,
  inputs_union.unix_ts,
  FROM_UNIXTIME(inputs_union.unix_ts) AS event_date
 WHERE
  inputs_union.eventid IS NULL
  OR
  -- we are saying here usernames can't be empty strings
  inputs_union.username IS NULL OR LENGTH(inputs_union.username) = 0
  OR
  -- we check this is a valid number by trying to convert it next
  inputs_union.unix_ts IS NULL
  OR
  FROM_UNIXTIME(inputs_union.unix_ts) IS NULL
-- second insert for good rows
-- again we deduplicate with landing so no rows get re-inserted
INSERT OVERWRITE TABLE ${hiveconf:good_table}
 SELECT DISTINCT
  inputs_union.eventid,
  inputs_union.username,
  inputs_union.unix_ts,
  FROM_UNIXTIME(inputs_union.unix_ts) AS event_date
 WHERE
  inputs_union.eventid IS NOT NULL
  AND
  -- we are saying here usernames can't be empty strings
  inputs_union.username IS NOT NULL AND LENGTH(inputs_union.username) > 0
  AND
  -- we check this is a valid number by trying to convert it next
  inputs_union.unix_ts IS NOT NULL
  AND
  FROM_UNIXTIME(inputs_union.unix_ts) IS NOT NULL;

