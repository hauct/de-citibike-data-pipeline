{{ config(materialized='table') }}

WITH citibike_data AS (
    SELECT * 
    FROM {{ ref ('staging_citibike_data')}}
),

dim_citibike_stations AS (
    SELECT *
    FROM {{ ref('dim_citibike_stations')}}
)

SELECT 
    ride_data.ride_id,			
    ride_data.rideable_type,	
    ride_data.member_casual,				
    ride_data.started_at ,			
    ride_data.ended_at ,
    TIMESTAMP_DIFF(ride_data.ended_at,ride_data.started_at,MINUTE) AS ride_duration,	
    ride_data.start_station_name,			
    start_station.station_id_int AS start_station_id,			
    ride_data.end_station_name,				
    end_station.station_id_int AS end_station_id,
    ride_data.start_lat,		
    ride_data.start_lng,			
    ride_data.end_lat,				
    ride_data.end_lng

FROM citibike_data AS ride_data
INNER JOIN dim_citibike_stations AS start_station
ON ride_data.start_station_name = start_station.name
INNER JOIN dim_citibike_stations AS end_station
ON ride_data.end_station_name = end_station.name