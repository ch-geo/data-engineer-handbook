WITH enumerated_game_details AS (
	SELECT *
	, ROW_NUMBER () OVER (PARTITION BY gd.game_id, gd.player_id, gd.team_id ORDER BY game_date_est) AS row_num
	FROM game_details gd
	JOIN games g
	ON g.game_id = gd.game_id
)
SELECT *
FROM enumerated_game_details
WHERE row_num = 1
