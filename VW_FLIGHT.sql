-- Set the current database to RECRUITMENT_DB
USE DATABASE RECRUITMENT_DB;

-- Set the current schema to candidate_00139
USE SCHEMA "CANDIDATE_00139";

CREATE OR REPLACE VIEW VW_FLIGHTS AS
SELECT 
    f."TRANSACTIONID",
    f."AIRLINECODE",
    a."AIRLINENAME",
    f."FLIGHTDATE",
    dd."DAY_DATE" AS "FLIGHTDATE_DATE",
    f."TAILNUM",
    f."FLIGHTNUM",
    f."ORIGINAIRPORTCODE",
    o."AIRPORTNAME" "ORIGINAIRPORTNAME",
    o."CITYNAME" "ORIGINACITYNAME",
    o."STATENAME" "ORIGINSTATENAME",
    f."DESTAIRPORTCODE",
    d."AIRPORTNAME" AS "DESTAIRPORTNAME",
    d."CITYNAME" AS "DESTCITYNAME",
    d."STATENAME" AS "DESTSTATENAME",
    f."CRSDEPTIME",
    f."DEPTIME",
    f."DEPDELAY",
    f."TAXIOUT",
    f."WHEELSOFF",
    f."WHEELSON",
    f."TAXIIN",
    f."CRSARRTIME",
    f."ARRTIME",
    f."ARRDELAY",
    f."CRSELAPSEDTIME",
    f."ACTUALELAPSEDTIME",
    f."CANCELLED",
    f."DIVERTED",
    f."DEPDELAYGT15",
    f."NEXTDAYARR",
    f."DISTANCE",
    f."DISTANCEGROUP"
FROM 
    FLIGHT_FACT f
LEFT JOIN DATE_DIM dd
    ON f.FLIGHTDATE = dd.flightdate
LEFT JOIN AIRLINE_DIM a 
    ON f.AIRLINECODE = a.AIRLINECODE
LEFT JOIN AIRPORT_DIM o 
    ON f.ORIGINAIRPORTCODE = o.AIRPORTCODE
LEFT JOIN AIRPORT_DIM d 
    ON f.DESTAIRPORTCODE = d.AIRPORTCODE;