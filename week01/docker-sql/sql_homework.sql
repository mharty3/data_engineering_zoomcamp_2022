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

-- It turns out answering how many rides were on the 15th was trickier than
-- I initially thought it would be: do you include only rides that were picked 
-- up on the 15th, only rides that were dropped off on the 15th, both?  It also 
-- looks like there are a lot of pick up and drop off times at exactly midnight 
-- (00:00:00), perhaps those represent missing data? Should those be included?
-- BUT...
-- With any of those choices, I'm not getting close to any of the options

----------------------------------------------------------------------------------------------------------------
-- QUESTION 4
SELECT tpep_pickup_datetime::date
      ,MAX(tip_amount) as max_tip_amount
FROM yellow_taxi_data as txd 
GROUP BY txd.tpep_pickup_datetime::date
ORDER BY max_tip_amount DESC
LIMIT 1;
--2021-01-20. A tip of 1140.44!

----------------------------------------------------------------------------------------------------------------
--QUESTION 5
with zone_names AS

(SELECT txd.pulocationid as pu_location_id
	    ,txd.dolocationid as do_location_id
        ,znpu.zone as pickup_zone
	    ,zndo.zone as dropoff_zone
FROM yellow_taxi_data as txd 
LEFT OUTER JOIN taxi_zone_lookup as znpu ON txd.pulocationid = znpu.locationid
LEFT OUTER JOIN taxi_zone_lookup as zndo ON txd.dolocationid = zndo.locationid
) 

SELECT pickup_zone, COUNT(pickup_zone) FROM zone_names
WHERE zone_names.dropoff_zone = 'Central Park'
GROUP BY zone_names.pickup_zone
ORDER BY count DESC
LIMIT 1
--Upper East Side North

----------------------------------------------------------------------------------------------------------------
-- QUESTION 6 attepmt 1
SELECT pulocationid
      ,dolocationid
	  ,avg(total_amount) as avg_total
FROM yellow_taxi_data txd
GROUP BY txd.pulocationid, txd.dolocationid
ORDER BY avg_total DESC
----------------------------------------------------------------------------------------------------------------
-- attempt 2
with zone_names AS
(SELECT txd.pulocationid as pu_location_id
	    ,txd.dolocationid as do_location_id
 		,txd.total_amount
        ,znpu.zone as pickup_zone
	    ,zndo.zone as dropoff_zone
FROM yellow_taxi_data as txd 
INNER JOIN taxi_zone_lookup as znpu ON txd.pulocationid = znpu.locationid
INNER JOIN taxi_zone_lookup as zndo ON txd.dolocationid = zndo.locationid
) 
SELECT pickup_zone
       ,dropoff_zone 
	  ,avg(total_amount) as avg_total
FROM zone_names
GROUP BY pickup_zone, dropoff_zone
ORDER BY avg_total DESC
--Alphabet City/Unknown