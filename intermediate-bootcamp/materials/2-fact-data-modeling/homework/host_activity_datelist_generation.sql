DO $$
DECLARE
    start_date DATE := '2023-01-01';
    end_date DATE := '2023-01-31';
    date_today DATE;

BEGIN
    date_today := start_date;
	TRUNCATE hosts_cumulated;
    
	WHILE date_today <= end_date LOOP
		WITH yesterday AS (
			SELECT *
			FROM hosts_cumulated
			WHERE current_day = date_today - INTERVAL '1 day'
		) 
		, enumerated_source AS (
			SELECT *
				, ROW_NUMBER () OVER (PARTITION BY user_id, device_id, host, event_time) AS row_num
				, CAST(event_time AS DATE) AS activity_date
			FROM events e
			WHERE CAST(event_time AS DATE) = date_today
		)
		, deduped_source AS (
			SELECT 
				user_id
				, device_id
				, host
				, activity_date
			FROM enumerated_source
			WHERE row_num = 1
			AND user_id IS NOT NULL
		)
		, today AS (
			SELECT
				host
				, ARRAY_AGG(ROW(user_id, activity_date)::host_activity_record) AS host_activity_datelist 
				, activity_date AS current_day
			FROM deduped_source
			GROUP BY host, activity_date
		)
		INSERT INTO hosts_cumulated (
			SELECT 
				COALESCE(t.host, y.host) AS host
				, y.host_activity_datelist || t.host_activity_datelist AS host_activity_datelist
				, COALESCE(t.current_day, y.current_day + 1) AS current_day
			FROM today t
			FULL OUTER JOIN yesterday y
			ON t.host = y.host 
		);
        RAISE NOTICE 'Processing date: %', date_today;
        date_today := date_today + INTERVAL '1 day';
    END LOOP;
END $$;