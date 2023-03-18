-- Merge the data from GCP into Snowflake table
MERGE INTO crime_data_uk.public.street_level_crime AS dest
USING
(
WITH staged_data AS (
    SELECT
          t.$1 AS crime_id
        , t.$2 AS month
        , t.$3 AS reported_by
        , t.$4 AS falls_within
        , t.$5 AS longitude
        , t.$6 AS latitude
        , t.$7 AS location
        , t.$8 AS lsoa_code
        , t.$9 AS lsoa_name
        , t.$10 AS crime_type
        , t.$11 AS last_outcome_category
        , t.$12 AS context
    FROM @crime_data_stage (file_format => 'gcs_csv_format') t
)
SELECT
    UUID_STRING() AS id
    , crime_id
    , month
    , reported_by
    , falls_within
    , longitude
    , latitude
    , location
    , lsoa_code
    , lsoa_name
    , crime_type
    , last_outcome_category
    , context
FROM staged_data
)
AS stg
ON dest.id = stg.id
WHEN MATCHED THEN
UPDATE SET
      dest.crime_id              = stg.crime_id
    , dest.month                 = stg.month
    , dest.reported_by           = stg.reported_by
    , dest.falls_within          = stg.falls_within
    , dest.longitude             = stg.longitude
    , dest.latitude              = stg.latitude
    , dest.location              = stg.location
    , dest.lsoa_code             = stg.lsoa_code
    , dest.lsoa_name             = stg.lsoa_name
    , dest.crime_type            = stg.crime_type
    , dest.last_outcome_category = stg.last_outcome_category
    , dest.context               = stg.context
WHEN NOT MATCHED THEN
INSERT
(
      id
    , crime_id
    , month
    , reported_by
    , falls_within
    , longitude
    , latitude
    , location
    , lsoa_code
    , lsoa_name
    , crime_type
    , last_outcome_category
    , context
)
VALUES
(
      stg.id
    , stg.crime_id
    , stg.month
    , stg.reported_by
    , stg.falls_within
    , stg.longitude
    , stg.latitude
    , stg.location
    , stg.lsoa_code
    , stg.lsoa_name
    , stg.crime_type
    , stg.last_outcome_category
    , stg.context
)
;

-- Get the Monthly count of each crime type for every street
SELECT
      month
    , lsoa_code
    , lsoa_name
    , crime_type
    , COUNT(crime_type) AS crimes_committed
FROM
    crime_data_uk.public.street_level_crime
WHERE
    lsoa_code IS NOT NULL
    AND crime_type IS NOT NULL
GROUP BY
      month
    , lsoa_code
    , lsoa_name
    , crime_type
ORDER BY
      lsoa_name
    , crimes_committed DESC
;


