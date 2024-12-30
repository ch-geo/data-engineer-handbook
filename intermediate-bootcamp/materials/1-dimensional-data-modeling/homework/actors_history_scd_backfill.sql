-- SCD backfill query
DO $$
BEGIN

TRUNCATE TABLE actors_history_scd;

WITH history_capture AS(
	SELECT *
		, LAG(quality_class, 1) OVER (PARTITION BY actorid ORDER BY current_year ASC) AS past_quality_class
		, LAG(is_active, 1) OVER (PARTITION BY actorid ORDER BY current_year ASC) AS past_is_active
	FROM ACTORS A
)
, chng_indicator AS (
	SELECT *
		, CASE 
			WHEN quality_class <> past_quality_class OR past_quality_class IS NULL THEN 1
			WHEN is_active <> past_is_active OR past_is_active IS NULL THEN 1
			ELSE 0
		END AS CHNG_FLG
	FROM history_capture
)
, streak_identifier AS (
	SELECT *
		, SUM(CHNG_FLG) OVER (PARTITION BY actorid ORDER BY current_year) AS streak_id
		FROM chng_indicator
)
INSERT INTO actors_history_scd (
	SELECT
		actor
		, actorid
		, quality_class
		, is_active
	  	, MIN(current_year) AS start_date
	  	, MAX(current_year) AS end_date
	FROM streak_identifier
	GROUP BY actor, actorid, quality_class, is_active, streak_id
	ORDER BY actor, start_date
);

END $$;
