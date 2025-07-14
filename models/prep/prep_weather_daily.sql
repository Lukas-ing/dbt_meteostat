WITH daily_data AS (
    SELECT * 
    FROM {{ref('staging_weather_daily')}}
),
add_features AS (
    SELECT *
		, To_char(date,'DD') AS date_day 		-- number of the day of month
		, to_char(date,'MM') AS date_month 	-- number of the month of year
		, to_char(date,'YYYY') AS date_year 		-- number of year
		, TO_char(date_part('week', date),'fm00') AS cw 			-- number of the week of year
--		, trim(to_char(date,'month')) AS month_name 
		, to_char(date,'FMmonth') AS month_name 	-- name of the month
		, trim(To_char(date,'day')) AS weekday 		-- name of the weekday
    FROM daily_data 
),
add_more_features AS (
    SELECT *,
		 CASE 
			WHEN trim(month_name) in ('december','january','february') THEN 'winter'
			WHEN month_name IN ('march','april','may')THEN 'spring'
            WHEN month_name IN ('june','july','august') THEN 'summer'
            WHEN month_name IN ('september','october','november') THEN 'autumn'
		END AS season
    FROM add_features
)
SELECT *
FROM add_more_features
ORDER BY date