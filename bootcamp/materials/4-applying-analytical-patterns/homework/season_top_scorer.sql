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
, max_points_per_season AS (
	SELECT 
		  season
		, MAX(total_points) AS max_total_points
	FROM game_details_aggregated
	WHERE season != '(Overall)' AND player_name != '(Overall)' AND team_abbreviation = '(Overall)'
	GROUP BY season
)
SELECT 
	  gdd.season
	, gdd.player_name
	, gdd.team_abbreviation
	, total_points
FROM game_details_aggregated gdd
INNER JOIN max_points_per_season mxpp
	ON gdd.season = mxpp.season 
	AND gdd.total_points = mxpp.max_total_points
WHERE gdd.season != '(Overall)' 
	AND gdd.player_name != '(Overall)' 
	AND gdd.team_abbreviation = '(Overall)'
ORDER BY season DESC
