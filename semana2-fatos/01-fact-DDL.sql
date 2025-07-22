-- em fatos, as colunas são ou dimensão ou medidas
create table fact_game_details (
    -- 'dim' para dimensões. Colunas com as quais se deve filtrar e agrupar.
    -- nem sempre fatos precisam conter apenas chaves para dimensões e métricas, alguns atributos descritivos podem ser úteis
    dim_game_date date,
    dim_season integer,
    dim_team_id integer,
    dim_player_id integer,
    dim_player_name text,
    dim_start_position text,
    dim_is_playing_at_home boolean,
    dim_not_play boolean,
    dim_not_dress boolean,
    dim_not_with_team boolean,

    -- 'm' para medidas (agregações e cálculos)
    m_minutes real,
    m_fgm integer,
    m_fga integer,
    m_fg3m integer,
    m_fg3a integer,
    m_ftm integer,
    m_fta integer,
    m_oreb integer,
    m_dreb integer,
    m_reb integer,
    m_ast integer,
    m_stl integer,
    m_blk integer,
    m_turnovers integer,
    m_pf integer,
    m_pts integer,
    m_plus_minus integer,

    primary key(dim_game_date, dim_player_id, dim_team_id)
);