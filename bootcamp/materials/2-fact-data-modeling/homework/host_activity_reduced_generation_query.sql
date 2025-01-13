DO $$
DECLARE
    start_date DATE := '2023-01-01';
    end_date DATE := '2023-01-31';
    date_today DATE;

BEGIN
    date_today := start_date;
	TRUNCATE host_activity_reduced;
    
	WHILE date_today <= end_date LOOP
		WITH past AS (
			SELECT *
			FROM host_activity_reduced
			WHERE month = DATE_TRUNC('month', date_today - INTERVAL '1 day')::DATE
		) 
		, today_unnested AS (
			SELECT 
				host
				, (UNNEST(host_activity_datelist)).user_id AS user_id
				, (UNNEST(host_activity_datelist)).activity_date AS activity_date
				, current_day
			FROM hosts_cumulated
			WHERE current_day = date_today
		)
		, todays_metrics AS (
			SELECT 
				host
				, COUNT(1) AS total_hits
				, COUNT(DISTINCT user_id) AS unique_visitors
				, DATE_TRUNC('month', current_day)::DATE AS month
				, current_day
			FROM today_unnested
			WHERE activity_date = current_day
			GROUP BY host, current_day
		)
		INSERT INTO host_activity_reduced (
			SELECT
				COALESCE(tm.host, p.host) AS host
				, COALESCE(tm.month, p.month) AS month
				, CASE
					WHEN p.hit_array IS NULL THEN ARRAY_FILL(0, ARRAY[current_day - tm.month]) || ARRAY[COALESCE(total_hits, 0)]
					ELSE p.hit_array || ARRAY[COALESCE(total_hits, 0)]
				END AS hit_array
				, CASE 
					WHEN p.unique_visitors_array IS NULL THEN ARRAY_FILL(0, ARRAY[current_day - tm.month]) || ARRAY[COALESCE(unique_visitors, 0)]
					ELSE p.unique_visitors_array || ARRAY[COALESCE(unique_visitors, 0)] 
				END AS unique_visitors_array
			FROM todays_metrics tm
			FULL OUTER JOIN past p
			ON tm.host = p.host
			AND tm.month = p.month
		)
		ON CONFLICT (host, month)
		DO 
			UPDATE SET
		    	hit_array = EXCLUDED.hit_array,
		    	unique_visitors_array = EXCLUDED.unique_visitors_array
		;
        RAISE NOTICE 'Processing date: %', date_today;
        date_today := date_today + INTERVAL '1 day';
    END LOOP;
END $$;