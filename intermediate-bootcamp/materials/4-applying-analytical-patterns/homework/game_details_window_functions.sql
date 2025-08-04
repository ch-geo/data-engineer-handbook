WITH game_details_aggregated AS (
	SELECT
	    gd.game_id
	  , COALESCE(gd.team_abbreviation, 'UNKNOWN') AS team_abbreviation
	  , g.home_team_wins AS team_won
	  FROM public.game_details gd
	  INNER JOIN public.games g
		  ON gd.game_id = g.game_id 
		  AND gd.team_id = g.home_team_id 
	  GROUP BY gd.game_id, gd.team_abbreviation, team_won
)
SELECT 
	  *
	, SUM(team_won) OVER (PARTITION BY team_abbreviation ORDER BY game_id ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) AS wining_streak
FROM game_details_aggregated;


WITH streaks_indicator AS (
	SELECT
	  game_id
	, player_name
	, COALESCE(pts, 0) AS pts
	, CASE WHEN pts > 10 THEN 1 ELSE 0 END AS pts_over_10
	FROM public.game_details
	WHERE player_name = 'LeBron James'
)
, streaks_augmented AS (
	SELECT 
		*
		, ROW_NUMBER() OVER (ORDER BY game_id) AS rn1
		, ROW_NUMBER() OVER (PARTITION BY pts_over_10 ORDER BY game_id) AS rn2
		FROM streaks_indicator
)
, streaks_marked AS (
	SELECT 
	  *
	, rn1 - rn2 + pts_over_10 AS streak_id
	FROM streaks_augmented
)
SELECT 
      game_id
    , player_name
    , pts
    , streak_id
    , COUNT(pts_over_10) OVER (PARTITION BY streak_id ORDER BY game_id) AS concecutive_match_over_10
FROM streaks_marked
WHERE pts_over_10 = 1
ORDER BY game_id;