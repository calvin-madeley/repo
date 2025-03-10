-- Set the current database to RECRUITMENT_DB
USE DATABASE "RECRUITMENT_DB";

-- Set the current schema to candidate_00139
USE SCHEMA "CANDIDATE_00139";

-- Create or replace a table to store flight data with specific columns
CREATE OR REPLACE TABLE "FLIGHT_RAW" (
    "TRANSACTIONID" VARCHAR(250),
    "FLIGHTDATE" DATE,
    "AIRLINECODE" VARCHAR(250),
    "AIRLINENAME" VARCHAR(250),
    "TAILNUM" VARCHAR(250),
    "FLIGHTNUM" VARCHAR(250),
    "ORIGINAIRPORTCODE" VARCHAR(250),
    "ORIGAIRPORTNAME" VARCHAR(250),
    "ORIGINCITYNAME" VARCHAR(250),
    "ORIGINSTATE" VARCHAR(250),
    "ORIGINSTATENAME" VARCHAR(250),
    "DESTAIRPORTCODE" VARCHAR(250),
    "DESTAIRPORTNAME" VARCHAR(250),
    "DESTCITYNAME" VARCHAR(250),
    "DESTSTATE" VARCHAR(250),
    "DESTSTATENAME" VARCHAR(250),
    "CRSDEPTIME" VARCHAR(250),    
    "DEPTIME" VARCHAR(250),       
    "DEPDELAY" NUMBER,
    "TAXIOUT" VARCHAR(250),       
    "WHEELSOFF" VARCHAR(250),     
    "WHEELSON" VARCHAR(250),      
    "TAXIIN" VARCHAR(250),        
    "CRSARRTIME" VARCHAR(250),    
    "ARRTIME" VARCHAR(250),       
    "ARRDELAY" NUMBER,
    "CRSELAPSEDTIME" NUMBER,
    "ACTUALELAPSEDTIME" NUMBER,
    "CANCELLED" VARCHAR(250),     
    "DIVERTED" VARCHAR(250),      
    "DISTANCE" STRING       
);

-- Load data into the FLIGHT_DATA table from a gzip compressed CSV file
COPY INTO "FLIGHT_RAW"
FROM @RECRUITMENT_DB.PUBLIC.S3_FOLDER/flights.gz
FILE_FORMAT = (TYPE = 'CSV', 
               FIELD_DELIMITER = '|', 
               FIELD_OPTIONALLY_ENCLOSED_BY = '"', 
               SKIP_HEADER = 1,
               COMPRESSION = 'GZIP');