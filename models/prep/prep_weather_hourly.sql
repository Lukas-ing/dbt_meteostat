    WITH hourly_data AS (
    SELECT * 
    FROM {{ref('staging_weather_hourly')}}
),
add_features AS (
    SELECT *
		, timestamp::DATE AS date               -- only date (hours:minutes:seconds) as DATE data type
		, timestamp::TIME  AS time                           -- only time (hours:minutes:seconds) as TIME data type
        , TO_CHAR(timestamp,'HH24:MI') as hour  -- time (hours:minutes) as TEXT data type
        , TO_CHAR(timestamp, 'FMmonth') AS month_name   -- month name as a TEXT
        , to_char(timestamp, 'day') AS weekday        -- weekday name as TEXT        
        , DATE_PART('day', timestamp) AS date_day
		, to_char(timestamp, 'month') AS date_month
		, to_char(timestamp, 'YYYY') AS date_year
		, TO_char(date_part('week', timestamp),'fm00') AS cw
    FROM hourly_data
),
add_more_features AS (
    SELECT *
		,(CASE 
			WHEN time BETWEEN '00:00' AND '04:00' THEN 'night'
			WHEN time BETWEEN '04:00' AND '18:00' THEN 'day'
			WHEN time BETWEEN '18:00' AND '21:00' THEN 'evening'
			WHEN time BETWEEN '21:00' AND '24:00' THEN 'night'
		END) AS day_part
    FROM add_features
)
SELECT *
FROM add_more_features