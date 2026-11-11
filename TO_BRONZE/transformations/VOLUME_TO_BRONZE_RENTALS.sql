CREATE OR REFRESH STREAMING TABLE workspace.bronze.b_rentals
COMMENT "Raw as-is data from volume"
AS SELECT *, current_timestamp() AS _ingest_timestamp
FROM STREAM read_files(
  '/Volumes/workspace/bronze/bronze_feeding/rentals.json',
  format => 'json',
  multiline => 'true',
  schema => '
    _id STRING,
    additionalCostsRaw STRING,
    areaSqm STRING,
    city STRING,
    crawlStatus STRING,
    crawledAt STRING,
    deposit STRING,
    descriptionTranslated STRING,
    detailsCrawledAt STRING,
    energyLabel STRING,
    firstSeenAt STRING,
    furnish STRING,
    gender STRING,
    internet STRING,
    isRoomActive STRING,
    kitchen STRING,
    lastSeenAt STRING,
    latitude STRING,
    living STRING,
    longitude STRING,
    matchCapacity STRING,
    pageDescription STRING,
    pageTitle STRING,
    pets STRING,
    postalCode STRING,
    postedAgo STRING,
    propertyType STRING,
    availability STRING,
    registrationCost STRING,
    rent STRING,
    roommates STRING,
    shower STRING,
    smokingInside STRING,
    source STRING,
    title STRING,
    toilet STRING
  ',
  schemaEvolutionMode => 'none'
);

--- Silver data for rentals
  CREATE OR REFRESH STREAMING TABLE workspace.silver.s_rentals (
  CONSTRAINT valid_latitude EXPECT (cast(latitude as DOUBLE) BETWEEN -90 AND 90) ON VIOLATION DROP ROW,
  CONSTRAINT valid_longitude EXPECT (cast(longitude as DOUBLE) BETWEEN -180 AND 180) ON VIOLATION DROP ROW,
  CONSTRAINT valid_postalCode EXPECT (zipcode IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Cleaned, deduplicated, and standardized Silver AirBnB rental data"
AS SELECT DISTINCT
  -- Standardize Dutch Zipcodes (4 digits + 2 uppercase letters)
  CASE
    WHEN UPPER(TRIM(postalCode)) RLIKE '^\\d{4}\\s?[A-Za-z]{2}$' 
      THEN REGEXP_REPLACE(UPPER(TRIM(postalCode)), '^(\\d{4})\\s?([A-Za-z]{2})$', '$1 $2')
    WHEN UPPER(TRIM(postalCode)) RLIKE '^\\d{4}$' 
      THEN UPPER(TRIM(postalCode))
    ELSE NULL
  END AS zipcode,
  city,
  CAST(latitude AS DOUBLE) AS latitude,
  CAST(longitude AS DOUBLE) AS longitude,
  propertyType,
  CAST(REGEXP_EXTRACT(rent, '([0-9]+)', 1) AS DECIMAL(10,2)) AS monthly_rent_eur,
  CAST(REGEXP_EXTRACT(areaSqm, '([0-9]+)', 1) AS INT) AS area_sqm,
  current_timestamp() AS silver_processed_at
FROM STREAM(workspace.bronze.b_rentals);