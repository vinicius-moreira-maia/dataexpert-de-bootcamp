CREATE OR REPLACE PROCEDURE atualiza_players(ano_ontem INTEGER, ano_hoje INTEGER)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO players
    WITH ontem AS (
        SELECT * FROM players
        WHERE current_season = ano_ontem
    ), 
    hoje AS (
        SELECT * FROM player_seasons
        WHERE season = ano_hoje
    )
    SELECT
        COALESCE(h.player_name, o.player_name) AS player_name,
        COALESCE(h.height, o.height) AS height,
        COALESCE(h.college, o.college) AS college,
        COALESCE(h.country, o.country) AS country,
        COALESCE(h.draft_year, o.draft_year) AS draft_year,
        COALESCE(h.draft_round, o.draft_round) AS draft_round,
        COALESCE(h.draft_number, o.draft_number) AS draft_number,
        
        CASE 
            WHEN o.seasons IS NULL THEN 
                ARRAY[
                    ROW(h.season, h.pts, h.ast, h.reb, h.weight)::season_stats
                ]
            WHEN h.season IS NOT NULL THEN 
                o.seasons || ARRAY[
                    ROW(h.season, h.pts, h.ast, h.reb, h.weight)::season_stats
                ]
            ELSE o.seasons
        END AS season_stats,

        COALESCE(h.season, o.current_season + 1) AS current_season,

        CASE
            WHEN h.season IS NOT NULL THEN 
                CASE 
                    WHEN h.pts > 20 THEN 'star'
                    WHEN h.pts > 15 THEN 'good'
                    WHEN h.pts > 10 THEN 'average'
                    ELSE 'bad'
                END::scoring_class
            ELSE o.scoring_class
        END AS scoring_class,

        CASE 
            WHEN h.season IS NOT NULL THEN 0
            ELSE o.years_since_last_season + 1
        END AS years_since_last_season

    FROM hoje h
    FULL OUTER JOIN ontem o
    ON h.player_name = o.player_name;
END;
$$;
