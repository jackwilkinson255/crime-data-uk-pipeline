terraform {
  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.59"
    }
  }
}

provider "snowflake" {
  role  = "SYSADMIN"
}

resource "snowflake_database" "sf_db" {
  name = upper(var.project_id)
}

resource "snowflake_warehouse" "sf_wh" {
  name           = "${upper(var.project_id)}_WH"
  warehouse_size = "large"
  auto_suspend = 60
}



provider "snowflake" {
    alias = "security_admin"
    role  = "SECURITYADMIN"
}

resource "snowflake_role" "role" {
    provider = snowflake.security_admin
    name     = "CDUK_SVC_ROLE"
}


resource "snowflake_database_grant" "grant" {
    provider          = snowflake.security_admin
    database_name     = snowflake_database.sf_db.name
    privilege         = "USAGE"
    roles             = [snowflake_role.role.name]
    with_grant_option = false
}

resource "snowflake_schema" "schema" {
    database   = snowflake_database.sf_db.name
    name       = "${upper(var.project_id)}_SCHEMA"
    is_managed = false
}

resource "snowflake_schema_grant" "grant" {
    provider          = snowflake.security_admin
    database_name     = snowflake_database.sf_db.name
    schema_name       = snowflake_schema.schema.name
    privilege         = "USAGE"
    roles             = [snowflake_role.role.name]
    with_grant_option = false
}

resource "snowflake_warehouse_grant" "grant" {
    provider          = snowflake.security_admin
    warehouse_name    = snowflake_warehouse.sf_wh.name
    privilege         = "USAGE"
    roles             = [snowflake_role.role.name]
    with_grant_option = false
}


######### STREET TABLE & STORED PROC #########
resource "snowflake_table" "street_table" {
  database            = snowflake_schema.schema.database
  schema              = snowflake_schema.schema.name
  name                = "STREET_LEVEL"
  change_tracking     = false

  column {
    name     = "ID"
    type     = "STRING"
    nullable = false
  }

  column {
    name     = "CRIME_ID"
    type     = "STRING"
    nullable = true
  }

  column {
    name     = "MONTH"
    type     = "STRING"
    nullable = true
  }

  column {
    name     = "REPORTED_BY"
    type     = "STRING"
    nullable = true
  }

  column {
    name     = "FALLS_WITHIN"
    type     = "STRING"
    nullable = true
  }

  column {
    name     = "LONGITUDE"
    type     = "NUMERIC(9,6)"
    nullable = true
  }

  column {
    name     = "LATITUDE"
    type     = "NUMERIC(9,6)"
    nullable = true
  }

  column {
    name     = "LOCATION"
    type     = "STRING"
    nullable = true
  }

  column {
    name     = "LSOA_CODE"
    type     = "STRING"
    nullable = true
  }

  column {
    name     = "LSOA_NAME"
    type     = "STRING"
    nullable = true
  }

  column {
    name     = "CRIME_TYPE"
    type     = "STRING"
    nullable = true
  }

  column {
    name     = "LAST_OUTCOME_CATEGORY"
    type     = "STRING"
    nullable = true
  }

  column {
    name     = "CONTEXT"
    type     = "STRING"
    nullable = true
  }

}

resource "snowflake_table_constraint" "primary_key" {
  name     = "STREET_KEY"
  type     = "PRIMARY KEY"
  table_id = snowflake_table.street_table.id
  columns  = ["ID"]
}


resource "snowflake_procedure" "street_proc" {
  name     = "STREETPROC"
  database = snowflake_database.sf_db.name
  schema   = snowflake_schema.schema.name
  language = "SQL"
  return_type         = "BOOLEAN"
  execute_as          = "CALLER"
  statement           = <<EOT

  DECLARE
      STATUS BOOLEAN DEFAULT FALSE;
  BEGIN
      MERGE INTO ${upper(var.project_id)}.${snowflake_schema.schema.name}.${snowflake_table.street_table.name} AS dest
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
          FROM @STREET_DATA_STAGE (file_format => GCS_CSV_FORMAT) t
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
  STATUS := TRUE;
  RETURN STATUS;
  END;
  EOT
}

