
--- QUESTION 3
SELECT COUNT(1)
FROM yellow_taxi_data 
 WHERE (tpep_pickup_datetime::date = DATE '2021-01-15'
        OR 
       tpep_dropoff_datetime::date = DATE '2021-01-15'
	   )
	   AND
	   (tpep_dropoff_datetime::time <> '00:00:00'
		AND
		tpep_pickup_datetime::time <> '00:00:00'
		)
--------------------------------------------------
-- QUESTION 3 option 2
SELECT *
FROM yellow_taxi_data 
 WHERE tpep_pickup_datetime::date = DATE '2021-01-15' 
        OR 
       tpep_dropoff_datetime::date = DATE '2021-01-15'

----------------------------------------------------------------------------------------------------------------
-- QUESTION 4
SELECT tpep_pickup_datetime::date
      ,MAX(tip_amount) as max_tip_amount
FROM yellow_taxi_data as txd 
GROUP BY txd.tpep_pickup_datetime::date
ORDER BY max_tip_amount DESC
LIMIT 1;
----------------------------------------------------------------------------------------------------------------
--QUESTION 5
with zone_names AS

(SELECT txd."PULocationID" as pu_location_id
	      ,txd."DOLocationID" as do_location_id
       ,znpu.zone as pickup_zone
	      ,zndo.zone as dropoff_zone
FROM yellow_taxi_data as txd 
LEFT OUTER JOIN taxi_zone_lookup as znpu ON txd."PULocationID" = znpu.locationid
LEFT OUTER JOIN taxi_zone_lookup as zndo ON txd."DOLocationID" = zndo.locationid
) 

SELECT pickup_zone, COUNT(pickup_zone) FROM zone_names
WHERE zone_names.dropoff_zone = 'Central Park'
GROUP BY zone_names.pickup_zone
ORDER BY count DESC
LIMIT 1
----------------------------------------------------------------------------------------------------------------
-- QUESTION 6 attepmt 1
SELECT "PULocationID"
      ,"DOLocationID" 
	  ,avg(total_amount) as avg_total
FROM yellow_taxi_data txd
GROUP BY txd."PULocationID", txd."DOLocationID"
ORDER BY avg_total DESC
----------------------------------------------------------------------------------------------------------------
-- attempt 2
with zone_names AS
(SELECT txd."PULocationID" as pu_location_id
	    ,txd."DOLocationID" as do_location_id
 		,txd.total_amount
        ,znpu.zone as pickup_zone
	    ,zndo.zone as dropoff_zone
FROM yellow_taxi_data as txd 
INNER JOIN taxi_zone_lookup as znpu ON txd."PULocationID" = znpu.locationid
INNER JOIN taxi_zone_lookup as zndo ON txd."DOLocationID" = zndo.locationid
) 
SELECT pickup_zone
       ,dropoff_zone 
	  ,avg(total_amount) as avg_total
FROM zone_names
GROUP BY pickup_zone, dropoff_zone
ORDER BY avg_total DESC