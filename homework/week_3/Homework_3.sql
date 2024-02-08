-- Query public available table
SELECT station_id, name FROM
    bigquery-public-data.new_york_citibike.citibike_stations
LIMIT 100;


CREATE EXTERNAL TABLE DATASET.NEW_TABLE
OPTIONS (
  format = 'CLOUD_BIGTABLE',
  uris = ['URI'],
  bigtable_options = BIGTABLE_OPTIONS );

-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `direct-raceway-412400.taxi_rides_ny.external_green_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://mage-mlzoomcamp/green_tripdata_2022-01.parquet',
          'gs://mage-mlzoomcamp/green_tripdata_2022-02.parquet',
          'gs://mage-mlzoomcamp/green_tripdata_2022-03.parquet',
          'gs://mage-mlzoomcamp/green_tripdata_2022-04.parquet',
          'gs://mage-mlzoomcamp/green_tripdata_2022-05.parquet',
          'gs://mage-mlzoomcamp/green_tripdata_2022-06.parquet',
          'gs://mage-mlzoomcamp/green_tripdata_2022-07.parquet',
          'gs://mage-mlzoomcamp/green_tripdata_2022-08.parquet',
          'gs://mage-mlzoomcamp/green_tripdata_2022-09.parquet',
          'gs://mage-mlzoomcamp/green_tripdata_2022-10.parquet',
          'gs://mage-mlzoomcamp/green_tripdata_2022-11.parquet',
          'gs://mage-mlzoomcamp/green_tripdata_2022-12.parquet'
          ]
);

-- Check trip data
SELECT * FROM taxi_rides_ny.external_green_tripdata limit 10;

-- Check trip data
SELECT COUNT(1) FROM taxi_rides_ny.external_green_tripdata;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE direct-raceway-412400.taxi_rides_ny.green_tripdata_non_partitoned AS
SELECT * FROM direct-raceway-412400.taxi_rides_ny.external_green_tripdata;

-- Check trip data
SELECT COUNT(1) FROM taxi_rides_ny.green_tripdata_non_partitoned;

-- Create materialized view
CREATE MATERIALIZED VIEW taxi_rides_ny.materialized_green_tripdata AS (
  SELECT VendorID, PULocationID
  FROM
    taxi_rides_ny.green_tripdata_non_partitoned
);

-- Count distinct PULocationID - external table
SELECT COUNT(DISTINCT PULocationID) FROM taxi_rides_ny.external_green_tripdata;

-- Count distinct PULocationID - matrliazed view 
SELECT COUNT(DISTINCT PULocationID) FROM taxi_rides_ny.green_tripdata_non_partitoned;


-- Count rows where fare_amount = 0
SELECT COUNT(1) FROM taxi_rides_ny.external_green_tripdata
WHERE fare_amount = 0;

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE taxi_rides_ny.green_tripdata_partitoned_clustered
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID AS
SELECT * FROM taxi_rides_ny.external_green_tripdata;

-- non partitioned data
-- 12.82 MB
SELECT DISTINCT(VendorID)
FROM taxi_rides_ny.green_tripdata_non_partitoned
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

-- partitioned data
-- 1.12 MB
SELECT DISTINCT(VendorID)
FROM taxi_rides_ny.green_tripdata_partitoned_clustered
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';