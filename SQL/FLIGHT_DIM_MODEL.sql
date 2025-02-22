-- Set the current database to RECRUITMENT_DB
USE DATABASE "RECRUITMENT_DB";

-- Set the current schema to candidate_00139
USE SCHEMA "CANDIDATE_00139";

-- Create or replace a table to store airport information
CREATE OR REPLACE TABLE "AIRPORT_DIM" (
        "AIRPORTCODE" VARCHAR(250) NOT NULL PRIMARY KEY
        , "AIRPORTNAME" VARCHAR(250)
        , "CITYNAME" VARCHAR(250)
        , "STATE" VARCHAR(250)
        , "STATENAME" VARCHAR(250)
    ) AS (
        WITH DEST_CTE AS (
            SELECT
                "ORIGINAIRPORTCODE" AS "AIRPORTCODE",
                TRIM(SPLIT_PART("ORIGAIRPORTNAME", ':', 2)) AS "AIRPORTNAME",
                "ORIGINCITYNAME" AS "CITYNAME",
                "ORIGINSTATE" AS "STATE",
                "ORIGINSTATENAME" AS "STATENAME"
            FROM
                "FLIGHT_RAW"
            GROUP BY
                "ORIGINAIRPORTCODE", "ORIGAIRPORTNAME", "ORIGINCITYNAME", "ORIGINSTATE", "ORIGINSTATENAME"
        ), ORIG_CTE AS (
            SELECT
                "DESTAIRPORTCODE" AS "AIRPORTCODE",
                TRIM(SPLIT_PART("DESTAIRPORTNAME", ':', 2)) AS "AIRPORTNAME",
                "DESTCITYNAME" AS "CITYNAME",
                "DESTSTATE" AS "STATE",
                "DESTSTATENAME" AS "STATENAME"
            FROM
                "FLIGHT_RAW"
            GROUP BY
                "DESTAIRPORTCODE", "DESTAIRPORTNAME", "DESTCITYNAME", "DESTSTATE", "DESTSTATENAME"
        )
        SELECT
            "AIRPORTCODE",
            "AIRPORTNAME",
            "CITYNAME",
            "STATE",
            "STATENAME"
        FROM (
            SELECT * FROM ORIG_CTE
            UNION ALL 
            SELECT * FROM DEST_CTE
        )
        GROUP BY
            "AIRPORTCODE", "AIRPORTNAME", "CITYNAME", "STATE", "STATENAME"
    );

-- Check for duplicate AIRPORTCODE in AIRPORT_DIM
SELECT COUNT(1) AS QTY, "AIRPORTCODE" FROM "AIRPORT_DIM" GROUP BY "AIRPORTCODE" ORDER BY 1 DESC;

-- Create or replace a table to store distinct airline information
CREATE OR REPLACE TABLE "AIRLINE_DIM" (
    "AIRLINECODE" VARCHAR(250) NOT NULL PRIMARY KEY
    , "AIRLINENAME" VARCHAR(250)
    , "AIRLINECOMMENTS" VARCHAR(250)
) AS (
    SELECT DISTINCT
        FR."AIRLINECODE",
        TRIM(SPLIT_PART(FR."AIRLINENAME", ':', 1)) AS "AIRLINENAME",
        TRIM(REPLACE(TRIM(SPLIT_PART(FR."AIRLINENAME", ':', 2)), FR."AIRLINECODE")) AS AIRLINECOMMENTS
    FROM
        "FLIGHT_RAW" FR
);

CREATE OR REPLACE TABLE DATE_DIM (
    "FLIGHTDATE" CHAR(10) NOT NULL PRIMARY KEY
    , "DAY_DATE" DATE
    , "YEAR" NUMBER(38,0)
    , "MONTH" NUMBER(38,0)
    , "MONTH_NAME" VARCHAR(250)
    , "DAY_OF_MON" NUMBER(38,0)
    , "DAY_OF_WEEK" VARCHAR(250)
    , "WEEK_OF_YEAR" NUMBER(38,0)
    , "DAY_OF_YEAR" NUMBER(38,0)
) AS (
    WITH RECURSIVE DATE_CTE AS (
        SELECT '1970-01-01'::DATE AS MY_DATE
        UNION ALL
        SELECT DATEADD(DAY, 1, MY_DATE)
        FROM DATE_CTE
        WHERE MY_DATE < CURRENT_DATE
    )
    SELECT 
        TO_CHAR(MY_DATE, 'YYYY-MM-DD') AS "FLIGHTDATE",
        MY_DATE AS "DAY_DATE",
        YEAR(MY_DATE) AS "YEAR",
        MONTH(MY_DATE) AS "MONTH",
        LEFT(MONTHNAME(MY_DATE), 3) AS "MONTH_NAME",
        DAY(MY_DATE) AS "DAY_OF_MON",
        DAYNAME(MY_DATE) AS "DAY_OF_WEEK",
        WEEKOFYEAR(MY_DATE) AS "WEEK_OF_YEAR",
        DAYOFYEAR(MY_DATE) AS "DAY_OF_YEAR"
    FROM DATE_CTE);

select * from "DATE_DIM";

-- Create a fact table to store flight information
CREATE OR REPLACE TABLE "FLIGHT_FACT" (
    "TRANSACTIONID" VARCHAR(250) NOT NULL PRIMARY KEY,
    "FLIGHTDATE" CHAR(10) NOT NULL REFERENCES "DATE_DIM"("FLIGHTDATE"),
    "AIRLINECODE" VARCHAR(250) NOT NULL REFERENCES "AIRLINE_DIM"("AIRLINECODE"),
    "TAILNUM" VARCHAR(250),
    "FLIGHTNUM" VARCHAR(250),
    "ORIGINAIRPORTCODE" VARCHAR(250) NOT NULL REFERENCES "AIRPORT_DIM"("AIRPORTCODE"),
    "DESTAIRPORTCODE" VARCHAR(250) NOT NULL REFERENCES "AIRPORT_DIM"("AIRPORTCODE"),
    "CRSDEPTIME" TIME(9),
    "DEPTIME" TIME(9),
    "DEPDELAY" NUMBER(38,0),
    "DEPDELAYGT15" NUMBER(1,0), -- Create binary field to indicate if departure delay is greater than 15 minutes
    "TAXIOUT" NUMBER(38,0),
    "WHEELSOFF" TIME(9),
    "WHEELSON" TIME,
    "TAXIIN" TIME(9),
    "CRSARRTIME" TIME(9),
    "ARRTIME" TIME(9),
    "ARRDELAY" NUMBER(38,0),
    "CRSELAPSEDTIME" NUMBER(38,0),
	"ACTUALELAPSEDTIME" NUMBER(38,0),
    "NEXTDAYARR" NUMBER(1,0), -- Create binary field to indicate if flight arrived the next day
    "CANCELLED" NUMBER(1,0),
    "DIVERTED" NUMBER(1,0),
    "DISTANCE" VARCHAR(250),
    "DISTANCEGROUP" VARCHAR(250)
) AS (
    WITH FLIGHT_PROC AS (
        SELECT
            "TRANSACTIONID",
            "FLIGHTDATE",
            "AIRLINECODE",
            CASE WHEN "TAILNUM" LIKE 'UNKNOW%' THEN NULL ELSE TRIM("TAILNUM", '@ -') END AS "TAILNUM", -- If TAILNUM starts with 'UNKNOW', set it to NULL, otherwise trim '@', spaces, and '-'
            "FLIGHTNUM",
            "ORIGINAIRPORTCODE",
            "DESTAIRPORTCODE",
            COALESCE(TRY_TO_TIME(LPAD("CRSDEPTIME", 4, '0') || '00','HH24MISS'), CASE WHEN "CRSDEPTIME" = '2400' THEN '00:00:00'::TIME ELSE NULL END) AS "CRSDEPTIME", -- Convert CRSDEPTIME to TIME type, pad with zeros, set '2400' to '00:00:00'
            COALESCE(TRY_TO_TIME(LPAD("DEPTIME", 4, '0') || '00','HH24MISS'), CASE WHEN "DEPTIME" = '2400' THEN '00:00:00'::TIME ELSE NULL END) AS "DEPTIME", -- Convert DEPTIME to TIME type, pad with zeros, set '2400' to '00:00:00'
            "DEPDELAY",
            "TAXIOUT",
            COALESCE(TRY_TO_TIME(LPAD("WHEELSOFF", 4, '0') || '00','HH24MISS'), CASE WHEN "WHEELSOFF" = '2400' THEN '00:00:00'::TIME ELSE NULL END) AS "WHEELSOFF", -- Convert WHEELSOFF to TIME type, pad with zeros, set '2400' to '00:00:00'
            COALESCE(TRY_TO_TIME(LPAD("WHEELSON", 4, '0') || '00','HH24MISS'), CASE WHEN "WHEELSON" = '2400' THEN '00:00:00'::TIME ELSE NULL END) AS "WHEELSON", -- Convert WHEELSON to TIME type, pad with zeros, set '2400' to '00:00:00'
            "TAXIIN",
            COALESCE(TRY_TO_TIME(LPAD("CRSARRTIME", 4, '0') || '00','HH24MISS'), CASE WHEN "CRSARRTIME" = '2400' THEN '00:00:00'::TIME ELSE NULL END) AS "CRSARRTIME", -- Convert CRSARRTIME to TIME type, pad with zeros, set '2400' to '00:00:00'
            COALESCE(TRY_TO_TIME(LPAD("ARRTIME", 4, '0') || '00','HH24MISS'), CASE WHEN "ARRTIME" = '2400' THEN '00:00:00'::TIME ELSE NULL END) AS "ARRTIME", -- Convert ARRTIME to TIME type, pad with zeros, set '2400' to '00:00:00'
            "ARRDELAY",
            TRY_CAST("CRSELAPSEDTIME" AS NUMBER) AS "CRSELAPSEDTIME", -- Convert CRSELAPSEDTIME to NUMBER type
            TRY_CAST("ACTUALELAPSEDTIME" AS NUMBER) AS "ACTUALELAPSEDTIME", -- Convert ACTUALELAPSEDTIME to NUMBER type
            CASE 
                WHEN UPPER("CANCELLED") IN ('1','T','TRUE') THEN 1
                WHEN UPPER("CANCELLED") IN ('0','F','FALSE') THEN 0
                ELSE NULL
            END AS "CANCELLED", -- Convert CANCELLED to binary (1 or 0)
            CASE 
                WHEN UPPER("DIVERTED") IN ('1','T','TRUE') THEN 1
                WHEN UPPER("DIVERTED") IN ('0','F','FALSE') THEN 0
                ELSE NULL
            END AS "DIVERTED", -- Convert DIVERTED to binary (1 or 0)
            "DISTANCE",
            TRY_CAST(TRIM(SPLIT_PART("DISTANCE", ' ', 1)) AS NUMBER) AS "MILESDISTANCE" -- Convert DISTANCE to numeric value
        FROM
            "FLIGHT_RAW"
    )
    SELECT
        "TRANSACTIONID",
        "FLIGHTDATE",
        "AIRLINECODE",
        "TAILNUM",
        "FLIGHTNUM",
        "ORIGINAIRPORTCODE",
        "DESTAIRPORTCODE",
        "CRSDEPTIME",
        "DEPTIME",
        "DEPDELAY",
        CASE WHEN "DEPDELAY" > 15 THEN 1 ELSE 0 END AS "DEPDELAYGT15", -- Create binary field to indicate if departure delay is greater than 15 minutes
        "TAXIOUT",
        "WHEELSOFF",
        "WHEELSON",
        "TAXIIN",
        "CRSARRTIME",
        "ARRTIME",
        "ARRDELAY",
        "CRSELAPSEDTIME",
        "ACTUALELAPSEDTIME",
        CASE 
            WHEN "ACTUALELAPSEDTIME" IS NULL THEN 0
            WHEN "ACTUALELAPSEDTIME" >= 1440 THEN 1
            WHEN "ARRTIME" > "DEPTIME" THEN 0
            ELSE 1
        END AS "NEXTDAYARR", -- Create binary field to indicate if flight arrived the next day
        "CANCELLED",
        "DIVERTED",
        "DISTANCE",
        --MILESDISTANCE,
        CASE  
            WHEN TRY_CAST(TRIM(SPLIT_PART("DISTANCE", ' ', 1)) AS NUMBER) <= 100 THEN '0-100 miles'
            ELSE TO_VARCHAR(101 + (FLOOR((TRY_CAST(TRIM(SPLIT_PART("DISTANCE", ' ', 1)) AS NUMBER) - 101) / 100) * 100)) 
                    || '-' || 
                    TO_VARCHAR(101 + (FLOOR((TRY_CAST(TRIM(SPLIT_PART("DISTANCE", ' ', 1)) AS NUMBER) - 101) / 100) * 100) + 99) 
                    || ' miles'
        END AS "DISTANCEGROUP" -- Categorize distances into groups
    FROM
        FLIGHT_PROC
);