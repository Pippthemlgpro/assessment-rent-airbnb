-- Define a streaming table to ingest data from a volume
CREATE OR REFRESH STREAMING TABLE workspace.bronze.b_airbnb
COMMENT "Raw as-is data from volume"
AS SELECT *, current_timestamp() AS bronze_ingest_timestamp
-- defined path and csv details
FROM STREAM read_files(
  '/Volumes/workspace/bronze/bronze_feeding/airbnb.csv',
  format => "csv",
  header => "true",
  delimiter => ",",
  schema => """
    zipcode STRING,
    latitude STRING,
    longitude STRING,
    room_type STRING,
    accommodates STRING,
    bedrooms STRING,
    price STRING,
    review_scores_value STRING
  """,
  schemaEvolutionMode => "none");

--- Silver data for airbnb
  CREATE OR REFRESH STREAMING TABLE workspace.silver.s_airbnb (
  CONSTRAINT valid_latitude EXPECT (cast(latitude as DOUBLE) BETWEEN -90 AND 90) ON VIOLATION DROP ROW,
  CONSTRAINT valid_longitude EXPECT (cast(longitude as DOUBLE) BETWEEN -180 AND 180) ON VIOLATION DROP ROW,
  CONSTRAINT valid_price EXPECT (price_in_eur > 0)
)
COMMENT "Cleaned, deduplicated, and standardized Silver AirBnB rental data"
AS SELECT
DISTINCT
  -- Standardize Dutch Zipcodes (4 digits + 2 uppercase letters)
  CASE
    WHEN UPPER(TRIM(zipcode)) RLIKE '^\\d{4}\\s?[A-Za-z]{2}$' 
      THEN REGEXP_REPLACE(UPPER(TRIM(zipcode)), '^(\\d{4})\\s?([A-Za-z]{2})$', '$1 $2')
    WHEN UPPER(TRIM(zipcode)) RLIKE '^\\d{4}$' 
      THEN UPPER(TRIM(zipcode))
    ELSE NULL
  END AS zipcode,
  
  CAST(latitude AS DOUBLE) AS latitude,
  CAST(longitude AS DOUBLE) AS longitude,
  room_type,
  CAST(accommodates AS INT) AS accommodates,
  CAST(COALESCE(bedrooms, 0) AS INT) AS bedrooms,
  CAST(price AS DECIMAL(10, 2)) AS price_in_eur,
  CAST(review_scores_value AS FLOAT) AS review_scores_value,
  (review_scores_value IS NOT NULL) AS has_reviews,
  current_timestamp() AS silver_processed_at

FROM STREAM(workspace.bronze.b_airbnb);