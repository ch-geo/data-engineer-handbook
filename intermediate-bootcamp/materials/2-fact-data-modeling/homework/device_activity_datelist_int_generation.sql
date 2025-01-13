WITH USERS_UNNESTED AS (
	SELECT 
		user_id
		, (UNNEST(device_activity_datelist)).browser_type as browser_type
		, (UNNEST(device_activity_datelist)).activity_date as activity_date
		, current_day
	FROM user_devices_cumulated
	WHERE current_day = '2023-01-31'
)
, USERS_AGGREGATED AS (
	SELECT 
		user_id
		, browser_type
		, ARRAY_AGG(activity_date) as activity_records
		, current_day
	FROM USERS_UNNESTED
	GROUP BY user_id, browser_type, current_day
)
, SERIES AS (
	SELECT GENERATE_SERIES('2023-01-01', '2023-01-31', INTERVAL '1 DAY')::DATE AS series_date
)
, LATEST_ACTIVITY_INT_INDICATORS AS (
	SELECT 
		*
		, CASE
		   	WHEN activity_records @> ARRAY [s.series_date] THEN (POW(2, 30 - (current_day - series_date)))
		    ELSE 0
		  END AS LATEST_ACTIVITY_INT_INDICATOR
	FROM USERS_AGGREGATED ua
	CROSS JOIN SERIES s
)
SELECT
	USER_ID
	, BROWSER_TYPE
	, CAST(SUM(LATEST_ACTIVITY_INT_INDICATOR) AS BIGINT)::BIT(30) AS DATELIST_INT
FROM LATEST_ACTIVITY_INT_INDICATORS
GROUP BY USER_ID, BROWSER_TYPE