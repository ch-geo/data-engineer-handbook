WITH game_details_augmented AS (
	SELECT
	    COALESCE(g.season, -1) AS season
	  , COALESCE(gd.player_name, 'UNKNOWN') AS player_name
	  , COALESCE(gd.team_abbreviation, 'UNKNOWN') AS team_abbreviation
	  , gd.pts as points
	  FROM public.game_details gd
	  INNER JOIN public.games g
	  ON gd.game_id = g.game_id
)
, game_details_aggregated AS (
	SELECT
		COALESCE(season::TEXT, '(Overall)') AS season
	, COALESCE(player_name, '(Overall)') AS player_name
	, COALESCE(team_abbreviation, '(Overall)') AS team_abbreviation
	, SUM(CASE WHEN points IS NOT NULL THEN points ELSE 0 END) AS total_points
	FROM game_details_augmented
	GROUP BY GROUPING SETS (
	(player_name, team_abbreviation),
	(player_name, season),
	(team_abbreviation)
	)
)
SELECT *
FROM game_details_aggregated
WHERE season = '(Overall)' AND player_name != '(Overall)' AND team_abbreviation != '(Overall)'
ORDER BY total_points DESC
LIMIT 1