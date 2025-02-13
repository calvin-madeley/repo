Certainly! Below is an updated version of the `README.md` file with a focus on the **data quality issues** identified in the dataset and the logic for the creation of the additional columns `NEXTDAYARR` and `DEPDELAYGT15`.

---

# Flight Data Ingestion to Snowflake

## Overview
This project involves ingesting flight data from a compressed `.gz` file (`flights.gz`) stored in an external stage on Snowflake. The dataset contains information about flight transactions, airports, departure/arrival times, delays, and more. The data undergoes cleaning to address various data quality issues before being loaded into Snowflake for analysis.

## Project Structure

- **External Stage**: The file `flights.gz` is stored in the external stage `@RECRUITMENT_DB.PUBLIC.S3_FOLDER` in Snowflake.
- **File Format**: The file is a compressed CSV file with the `.gz` extension. The columns in the CSV file are delimited by a pipe (`|`), and the first row contains column headers.

## Data Quality Issues

### 1. **AIRPORTNAME Contains Extra Location Information**
   The `ORIGAIRPORTNAME` and `DESTAIRPORTNAME` columns contain location information in addition to the airport name. For example, a field value could look like this:

   ```
   "AlbuquerqueNM: Albuquerque International Sunport"
   ```

   This includes the city and state along with the airport name. The goal is to **cleanse this column** to only retain the airport name (e.g., "Albuquerque International Sunport").

### 2. **AIRLINENAME Contains Extra Airline Code and Comments**
   The `AIRLINENAME` column contains extra airline code information and sometimes comments that are not needed for analysis. For example:

   ```
   "Southwest Airlines Co.: WN"
   ```

   The `: WN` part is the airline code, which can be removed to just have the full airline name (e.g., "Southwest Airlines Co.").

### 3. **TAILNUM Contains Invalid Values ("UNKNOW") and Padding**
   The `TAILNUM` column contains values such as "UNKNOW" which should be `NULL` since it represents unknown or missing data. Additionally, it contains padding characters like '@', ' ', and '-' that should be removed to clean the value.

### 4. **CRSDEPTIME Contains Malformed 24-Hour Time (2400)**
   The `CRSDEPTIME` column occasionally contains a value of `2400` which is an invalid 24-hour time format. This should be corrected to `0000` to represent midnight.

### 5. **DIVERTED and CANCELLED Columns Contain Inconsistent Entries**
   The `DIVERTED` and `CANCELLED` columns have inconsistent values representing `TRUE` and `FALSE`. Some entries are:

   - `'T'`, `'True'`, `1` for TRUE
   - `'F'`, `'False'`, `0` for FALSE

   These should be standardized to `1` and `0` for consistency.

## Data Transformation Logic

### Cleaning Logic for Data Quality Issues

1. **AIRPORTNAME (Cleaning location information)**:
   - Use `TRIM` and `SPLIT` to remove the location information from `ORIGAIRPORTNAME` and `DESTAIRPORTNAME`, leaving only the airport name.
   - Example: `TRIM(SPLIT_PART("DESTAIRPORTNAME", ':', 2))`

2. **AIRLINENAME (Cleaning airline code and comments)**:
   - Use `TRIM` and `SPLET` to remove any airline code and extra comments from the `AIRLINENAME` field.
   - Example: `TRIM(SPLIT_PART(FR."AIRLINENAME", ':', 1)) AS "AIRLINENAME"`

3. **TAILNUM (Removing "UNKNOW" and Padding)**:
   - Replace `"UNKNOW"` with `NULL`.
   - Remove padding characters (`@`, spaces, `-`).
   - Example: `NULLIF(REGEXP_REPLACE(TAILNUM, '[@ -]', ''), 'UNKNOW')`

4. **CRSDEPTIME (Fix malformed 24-hour time)**:
   - Replace any instances of `2400` with `0000` in `CRSDEPTIME`.
   - Example: `COALESCE(TRY_TO_TIME(LPAD("DEPTIME", 4, '0') || '00','HH24MISS'), CASE WHEN "DEPTIME" = '2400' THEN '00:00:00'::TIME ELSE NULL END)`

5. **DIVERTED and CANCELLED (Standardizing TRUE/FALSE values)**:
   - Normalize the entries for both `DIVERTED` and `CANCELLED` to either `'1'` or `'0'`.
   - Example:
     ```sql
     CASE
         WHEN DIVERTED IN ('T', 'True', '1') THEN '1'
         WHEN DIVERTED IN ('F', 'False', '0') THEN '0'
         ELSE 'UNKNOWN'
     END
     ```

### Logic for Creating Additional Columns

1. **NEXTDAYARR**:
   - The `NEXTDAYARR` column is created to indicate whether the arrival time (`ARRTIME`) is on the next day compared to the departure time (`DEPTIME`).
   - This can be determined by comparing the times and checking if the arrival time is earlier than the departure time, or `ACTUALELAPSEDTIME` is greater than 1440 minutes (24 hours), indicating that the flight arrived on the following day.
   
   ```sql
        CASE 
            WHEN "ACTUALELAPSEDTIME" IS NULL THEN 0
            WHEN "ACTUALELAPSEDTIME" >= 1440 THEN 1
            WHEN "ARRTIME" > "DEPTIME" THEN 0
            ELSE 1
        END AS "NEXTDAYARR"
   ```

2. **DEPDELAYGT15**:
   - The `DEPDELAYGT15` column is created to flag whether a flight has a departure delay greater than 15 minutes.
   - If the `DEPDELAY` is greater than 15, this column will be `'TRUE'`, otherwise `'FALSE'`.

   ```sql
   CASE
       WHEN DEPDELAY > 15 THEN 'TRUE'
       ELSE 'FALSE'
   END AS DEPDELAYGT15
   ```