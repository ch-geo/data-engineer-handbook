-- Cumulative table generation query
DO $$
DECLARE
    start_year INT := 1970;  -- Initial year to process
    end_year INT := 2021;    -- Final year to process
BEGIN
	TRUNCATE TABLE actors;
	FOR year_to_process IN start_year..end_year LOOP
		WITH yesterday AS (
			SELECT 
				actor
				, actorid
				, films
				, quality_class
				, current_year
			FROM actors
			WHERE current_year = year_to_process - 1
		),
		today AS (
			SELECT 
				actor
				, actorid
				, ARRAY_AGG(ROW(film, votes, rating, filmid)::film_record) AS film_info
				, AVG(rating) AS avg_rating 
				, year
			FROM actor_films
			WHERE year = year_to_process
			GROUP BY actor, actorid, year
		)
		INSERT INTO actors (
				SELECT
					COALESCE (t.actor, y.actor) AS actor
					, COALESCE (t.actorid, y.actorid) AS actorid
					, y.films || t.film_info AS films
					, 
						CASE 
							WHEN t.avg_rating IS NULL THEN y.quality_class
							WHEN t.avg_rating > 8 THEN 'star' 
							WHEN t.avg_rating > 7 AND t.avg_rating <= 8 THEN 'good'
							WHEN t.avg_rating > 6 AND t.avg_rating <= 7 THEN 'average'
							ELSE 'bad'
						END::actor_rating AS quality_class
					, CASE WHEN t.year IS NULL THEN FALSE ELSE TRUE END AS is_active
					, COALESCE(t.year, y.current_year + 1) AS current_year
			FROM today t 
			FULL OUTER JOIN yesterday y 
			ON t.actor = y.actor
			AND t.actorid = y.actorid
		);

	   	RAISE NOTICE 'Processed year %', year_to_process;
	END LOOP;
END $$;
