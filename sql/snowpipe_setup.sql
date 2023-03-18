-- Queries used to setup Snowpipe connected to Google Cloud Storage
CREATE STORAGE INTEGRATION CRIME_DATA_INTEGRATION
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'GCS'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://data-police-uk-hastings')
;

DESC STORAGE INTEGRATION CRIME_DATA_INTEGRATION;


-- Creating notification for GCP subscriber to topic
CREATE NOTIFICATION INTEGRATION CRIME_DATA_NOTIFICATION_INTEGRATION
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = GCP_PUBSUB
  ENABLED = true
  GCP_PUBSUB_SUBSCRIPTION_NAME = 'projects/hastings-direct-interview/subscriptions/crime-data-bucket-subscriber';

DESC NOTIFICATION INTEGRATION CRIME_DATA_NOTIFICATION_INTEGRATION;


USE SCHEMA crime_data_uk.public;


CREATE STAGE CRIME_DATA_STAGE
  URL='gcs://data-police-uk-hastings'
  STORAGE_INTEGRATION = CRIME_DATA_INTEGRATION;


CREATE PIPE CRIME_DATA_PIPE
  AUTO_INGEST = true
  INTEGRATION = 'CRIME_DATA_NOTIFICATION_INTEGRATION'
  AS
COPY INTO crime_data_uk.public.street_level_crime
  FROM @crime_data_uk.public.crime_data_stage;


-- List the staged files to check we have registered it
LIST @crime_data_stage;


-- Create a file format.
CREATE OR REPLACE FILE FORMAT gcs_csv_format
    TYPE = 'csv'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1;