-- Snowflake database setup
CREATE OR REPLACE DATABASE CRIME_DATA_UK;

USE DATABASE CRIME_DATA_UK;

CREATE TABLE IF NOT EXISTS street_level_crime (
  id STRING UNIQUE PRIMARY KEY
, crime_id STRING DEFAULT NULL
, month STRING DEFAULT NULL
, reported_by STRING DEFAULT NULL
, falls_within STRING DEFAULT NULL
, longitude NUMERIC(9,6) DEFAULT NULL
, latitude NUMERIC(9,6) DEFAULT NULL
, location STRING DEFAULT NULL
, lsoa_code STRING DEFAULT NULL
, lsoa_name STRING DEFAULT NULL
, crime_type STRING DEFAULT NULL
, last_outcome_category STRING DEFAULT NULL
, context STRING DEFAULT NULL
)
;

CREATE OR REPLACE WAREHOUSE CRIME_DATA_UK_WAREHOUSE WITH
  warehouse_size='X-SMALL'
  auto_suspend = 120
  auto_resume = true
  initially_suspended=true;

USE WAREHOUSE CRIME_DATA_UK_WAREHOUSE;