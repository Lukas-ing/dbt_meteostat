SELECT 
p.airport_code, p.cw,
round(avg(p.avg_temp_c),1) AS avg_temp_c,
min(p.min_temp_c) AS min_temp_c,
max(p.max_temp_c) AS max_temp_c,
sum(p.precipitation_mm) AS precipitation_mm,
max(p.MAX_SNOW_MM ) AS max_snow_mm,
round(avg(p.AVG_WIND_DIRECTION),2) AS avg_wind_direction,
round(avg(p.AVG_WIND_SPEED_KMH),1) AS avg_wind_speed_kmh,
max(p.WIND_PEAKGUST_KMH) AS wind_peakgust_kmh,
round(avg(p.AVG_PRESSURE_HPA),2) AS avg_pressure_hpa,
sum(p.SUN_MINUTES) AS sun_minutes
FROM {{ref('prep_weather_daily')}} P
GROUP BY p.cw, p.AIRPORT_CODE
ORDER BY p.AIRPORT_CODE,p.cw