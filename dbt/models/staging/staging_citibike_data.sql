{{ config(materialized='view') }}

SELECT 
--Identifiers
CAST(ride_id AS string) AS ride_id,			
CAST(rideable_type AS string) AS rideable_type,	
CAST(member_cASual AS string) AS member_cASual,				

--Timestamps
CAST(started_at AS TIMESTAMP) AS started_at,			
CAST(ended_at AS TIMESTAMP) AS ended_at,

--station information
CAST(start_station_name AS string)	AS start_station_name,			
CAST(start_station_id AS numeric) AS 	start_station_id,			
CAST(end_station_name AS string) AS end_station_name,				
CAST(end_station_id AS numeric) AS end_station_id,

--lat ,lng info
CAST(start_lat AS numeric)	AS start_lat,		
CAST(start_lng AS numeric) AS start_lng,			
CAST(end_lat AS numeric) AS end_lat,				
CAST(end_lng AS numeric) AS end_lng				

FROM {{source('staging','citibike_tripsdata_raw')}}
WHERE ride_id IS NOT NULL AND ride_id <> 'nan'
AND start_station_name IS NOT NULL AND start_station_name <> 'nan'
AND end_station_name IS NOT NULL AND end_station_name <> 'nan' 
AND started_at IS NOT NULL
AND ended_at IS NOT NULL