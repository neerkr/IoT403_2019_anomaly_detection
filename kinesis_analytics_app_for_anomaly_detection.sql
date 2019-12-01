-- ** Anomaly detection **
-- Compute an anomaly score for each record in the source stream using Random Cut Forest
-- Creates a temporary stream and defines a schema
CREATE OR REPLACE STREAM "TEMP_STREAM" (
   "COL_timestamp" TIMESTAMP,   
   "geo_location" VARCHAR(32),
   "turbine_id" INTEGER,
   "wind_speed"     INTEGER,
   "oil_temperature"     INTEGER,
   "temperature"     INTEGER,
   "wind_direction"     INTEGER,
   "oil_level"     INTEGER,
   "RPM_blade"     INTEGER,
   "vibration_frequency" INTEGER,
   "ANOMALY_SCORE"  DOUBLE);
   
-- Creates an output stream and defines a schema
CREATE OR REPLACE STREAM "DESTINATION_SQL_STREAM" (
   "COL_timestamp" TIMESTAMP,   
   "geo_location" VARCHAR(32),
   "turbine_id" INTEGER,
   "wind_speed"     INTEGER,
   "oil_temperature"     INTEGER,
   "temperature"     INTEGER,
   "wind_direction"     INTEGER,
   "oil_level"     INTEGER,
   "RPM_blade"     INTEGER,
   "vibration_frequency" INTEGER,
   "ANOMALY_SCORE"  DOUBLE);
   
-- Creates an output stream and defines a schema - this stream will be used to send data to a Lambda Function.
CREATE OR REPLACE STREAM "DESTINATION_SQL_STREAM_Lambda" (
   "COL_timestamp" TIMESTAMP,   
   "geo_location" VARCHAR(32),
   "turbine_id" INTEGER,
   "wind_speed"     INTEGER,
   "oil_temperature"     INTEGER,
   "temperature"     INTEGER,
   "wind_direction"     INTEGER,
   "oil_level"     INTEGER,
   "RPM_blade"     INTEGER,
   "vibration_frequency" INTEGER,
   "ANOMALY_SCORE"  DOUBLE);
 
-- Compute an anomaly score for each record in the source stream
-- using Random Cut Forest
CREATE OR REPLACE PUMP "STREAM_PUMP" AS INSERT INTO "TEMP_STREAM"
SELECT STREAM "COL_timestamp", "geo_location", "turbine_id", "wind_speed", "oil_temperature","temperature", "wind_direction","oil_level","RPM_blade","vibration_frequency","ANOMALY_SCORE" FROM
  TABLE(RANDOM_CUT_FOREST(
    CURSOR(SELECT STREAM * FROM "SOURCE_SQL_STREAM_001")
  )
);

-- Sort records by descending anomaly score, insert into output stream
CREATE OR REPLACE PUMP "OUTPUT_PUMP" AS INSERT INTO "DESTINATION_SQL_STREAM"
SELECT STREAM * FROM "TEMP_STREAM"
ORDER BY FLOOR("TEMP_STREAM".ROWTIME TO SECOND), ANOMALY_SCORE DESC;


-- Sort records by descending anomaly score, insert into output stream
CREATE OR REPLACE PUMP "OUTPUT_PUMP2" AS INSERT INTO "DESTINATION_SQL_STREAM_Lambda"
SELECT STREAM * FROM "TEMP_STREAM"
ORDER BY FLOOR("TEMP_STREAM".ROWTIME TO SECOND), ANOMALY_SCORE DESC;

