-- dimensões com scd mantém histórico dos atributos (dos que forem preciso)

/*
    1- carregar a dimensão com scd toda de uma vez
    2- carregar de forma incremental
*/

create table players_scd (
    player_name TEXT,

    -- essas duas colunas terão histórico controlado
    scoring_class scoring_class,
    is_active boolean,

    start_season integer,
    end_season integer,

    current_season integer,
    primary key(player_name, start_season, end_season)
)

-- Essa inserção é do histórico completo!
insert into players_scd
-- essa cte trás o estado anterior das colunas 'is_active' e 'scoring_class' usando uma função de janela
with with_previous as (
    select 
        player_name,
        current_season,
        scoring_class,   
        is_active,
        lag(scoring_class) over (partition by player_name order by current_season) as previous_scoring_class,
        lag(is_active) over (partition by player_name order by current_season) as previous_is_active 
    from players
    where current_season <= 2021
),
-- essa cte trás os indicadores de mudanças nos estados
with_indicators as (
    select
        *,
        case
            -- se um OU o outro mudar
            when scoring_class <> previous_scoring_class then 1
            when is_active <> previous_is_active then 1
            else 0
        end as change_indicator
    from with_previous
),
-- 'streak' é sequência, continuidade, fase ...
with_streaks as (
    select 
        *,
        -- esse somatório me diz o quanto houveram mudanças ao longo do tempo
        sum(change_indicator) over (partition by player_name order by current_season) as streak_identifier
    from with_indicators
)

/*
Quando eu faço essas agregações eu sei a data que o jogador começou e que encerrou, e a coluna 'streak_identifier' já é um somatório agregado com função de janela vindo da cte 'with_streaks'.
*/

-- essa consulta consolida a informação que eu quero
-- ele vai trazer APENAS as temporadas em que houveram alteração de estado, ou seja, sem repetição em 'streak_identifier'
select 
    player_name, scoring_class,
    is_active,
    streak_identifier, 
    min(current_season) as start_season,
    max(current_season) as end_season,
    2021 as current_season -- em pipeline isso seria parametrizável
from with_streaks
group by player_name, streak_identifier, is_active, scoring_class
order by player_name, streak_identifier;
