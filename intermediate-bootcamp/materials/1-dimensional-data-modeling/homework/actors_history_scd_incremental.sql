-- SCD incremental query 
DO $$
DECLARE
    start_year INT := 1970;  -- Initial year to process
    end_year INT := 2021;    -- Final year to process
BEGIN
	TRUNCATE TABLE actors_history_scd;
	FOR year_to_process IN start_year..end_year LOOP
		WITH last_year_scd AS (
			SELECT * 
			FROM actors_history_scd 
			WHERE end_date = year_to_process - 1
		)
		, today AS (
			SELECT 
				actor
				, actorid
				, quality_class
				, is_active
				, current_year
			FROM actors
			WHERE current_year = year_to_process
		)
		, unchanged_data AS (
			SELECT
				T.actor
				, T.actorid
				, T.quality_class
				, T.is_active
				, SCD.start_date
				, T.current_year as end_date
			FROM today T
			JOIN last_year_scd SCD
			ON T.actorid = SCD.actorid
			WHERE T.quality_class = SCD.quality_class
			AND T.is_active = SCD.is_active
		)
		, new_or_changed_data AS (
			SELECT 
				T.actor
				, T.actorid
				, T.quality_class
				, T.is_active
				, T.current_year as start_date
				, T.current_year as end_date
			FROM today T
			LEFT JOIN last_year_scd SCD
			ON T.actorid = SCD.actorid
			WHERE (T.quality_class <> SCD.quality_class 
			OR T.is_active <> SCD.is_active)
			OR SCD.actorid IS NULL
		)
		INSERT INTO actors_history_scd (
			SELECT * FROM unchanged_data
			UNION ALL 
			SELECT * FROM new_or_changed_data
		)
		ON CONFLICT (actorid, start_date)
		DO UPDATE SET
		    end_date = EXCLUDED.end_date;
		
	   	RAISE NOTICE 'Processed year %', year_to_process;
	END LOOP;
END $$;