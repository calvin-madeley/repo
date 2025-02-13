
-- Set the current database to RECRUITMENT_DB
USE DATABASE "RECRUITMENT_DB";

-- Set the current schema to candidate_00139
USE SCHEMA "CANDIDATE_00139";

-- Create or replace a transient table to hold raw flight data
CREATE OR REPLACE TRANSIENT TABLE "FLIGHTS_RAW" (TXT VARCHAR);

-- Load data into the FLIGHTS_RAW table from a gzip compressed CSV file
COPY INTO "FLIGHTS_RAW"
FROM @RECRUITMENT_DB.PUBLIC.S3_FOLDER/flights.gz
FILE_FORMAT = (TYPE = 'CSV', 
               FIELD_OPTIONALLY_ENCLOSED_BY = '"', 
               COMPRESSION = 'GZIP');

-- Select the first 2 rows from FLIGHTS_RAW to determine schema from header information
SELECT * FROM "FLIGHTS_RAW" LIMIT 2;