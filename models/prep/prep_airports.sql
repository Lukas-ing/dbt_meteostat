WITH airports_reorder AS (
    SELECT 	faa, 
    		name,
    		city,
    		country,
    		tz,
    		dst,
    		lat,
    		lon,
    		alt
    FROM {{ref('staging_airports')}}
)
SELECT * FROM airports_reorder