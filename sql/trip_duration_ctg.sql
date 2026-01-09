/* 
  Project: NYC Taxi
  Layer: Raw to Processed
  Description: Calculates trip duration and high-value flags.
  Author: Krishna | Version: 1.0
*/

SELECT 
    *,
    round((unix_timestamp(dropoff_time) - unix_timestamp(pickup_time)) / 60, 2) as trip_duration_min,
    CASE WHEN fare > 50 THEN 'High Value' ELSE 'Standard' END as fare_category
FROM clean_table;