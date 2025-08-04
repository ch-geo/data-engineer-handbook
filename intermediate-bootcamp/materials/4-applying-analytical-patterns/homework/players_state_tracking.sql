WITH yesterday AS (
    SELECT 
          player_name
        , seasons
        , current_season
        , years_since_last_active
    FROM bootcamp.nba_players
    WHERE current_season = 1996
)
, today AS (
    SELECT 
          player_name
        , seasons
        , current_season
        , years_since_last_active
    FROM bootcamp.nba_players
    WHERE current_season = 1997
)
        SELECT COALESCE(t.player_name, y.player_name)                           AS player_name,
                COALESCE(y.current_season, t.current_season)                    AS first_active_season,
                COALESCE(t.current_season, y.current_season)                    AS last_active_season,
                COALESCE(t.years_since_last_active, y.years_since_last_active)  AS years_since_last_active,
                CASE
                    WHEN y.player_name IS NULL THEN 'New'
                    WHEN t.years_since_last_active = 0 THEN 'Continued Playing'
                    WHEN y.years_since_last_active = 0 AND t.years_since_last_active = 1 THEN 'Retired'
                    WHEN y.years_since_last_active != 0 AND t.years_since_last_active = 0  THEN 'Returned from Retirement'
                    ELSE 'Stayed Retired'
                END                                           as seasonal_state,
                COALESCE(t.seasons, y.seasons) AS seasons,
                t.current_season AS current_season
        FROM today t
        FULL OUTER JOIN yesterday y
            ON t.player_name = y.player_name
