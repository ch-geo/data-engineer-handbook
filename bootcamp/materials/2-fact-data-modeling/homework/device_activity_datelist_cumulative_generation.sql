DO $$
DECLARE
    start_date DATE := '2023-01-01';
    end_date DATE := '2023-01-31';
    date_today DATE;
BEGIN
    date_today := start_date;

    WHILE date_today <= end_date LOOP
		WITH yesterday AS (
			SELECT *
			FROM user_devices_cumulated
			WHERE current_day = date_today - INTERVAL '1 day'
		)
		, enumerated_source AS (
			SELECT *
			, ROW_NUMBER () OVER (PARTITION BY user_id, e.device_id, event_time) AS row_num
			, CAST(event_time AS DATE) AS activity_date
			FROM events e
			INNER JOIN devices d 
			ON e.device_id = d.device_id
			WHERE user_id IS NOT NULL
			AND e.device_id IS NOT NULL
			AND CAST(event_time AS DATE) = date_today
		)
		, deduped_source AS (
			SELECT 
				user_id
				, browser_type
				, activity_date
			FROM enumerated_source
			WHERE row_num = 1
			GROUP BY user_id, browser_type, activity_date
		)
		, today AS (
			SELECT 
				user_id
				, ARRAY_AGG(ROW(browser_type, activity_date)::activity_record) AS device_activity_datelist
				, activity_date
			FROM deduped_source
			GROUP BY user_id, activity_date
		)
		INSERT INTO user_devices_cumulated (
			SELECT 
				COALESCE(t.user_id, y.user_id) AS user_id
				, y.device_activity_datelist || t.device_activity_datelist
				, COALESCE(t.activity_date, y.current_day + 1) AS current_day
			FROM today t
			FULL OUTER JOIN yesterday y
			ON t.user_id = y.user_id
		);
        RAISE NOTICE 'Processing date: %', date_today;
        date_today := date_today + INTERVAL '1 day';
    END LOOP;
END $$;