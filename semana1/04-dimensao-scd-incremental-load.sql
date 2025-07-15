/*
INCREMENTAL LOAD !!

De forma geral:
    - Essa conulta pega apenas a partição mais recente dos dados dos dados históricos, que já estão na dimensão.
    - Os dados novos são comparados apenas com a última partição dos dados históricos.
    - Há 3 possibilidades nesse caso: ou o dado foi alterado ou o dado é novo ou não foi alterado.
*/

with last_season_scd as ( -- dado de "ontem"
    select * from players_scd
    where current_season = 2021
    and end_season = 2021
),
historical_scd as ( -- dado histórico
    select
        player_name, scoring_class, is_active, start_season, end_season 
    from players_scd
    where current_season = 2021
    and end_season < 2021
),
this_season_data as ( -- dado atual
    select * from players
    where current_season = 2022
),
unchanged_records as ( -- registros que não mudaram
    -- join para saber quem jogou nas duas temporadas e não mudou
    -- o jogador jogou as duas temporadas, então a temporada de início dele continua sendo a do registro anterior, e a última temporada passa a ser a temporada atual (adição de 1 ano aqui)
    select ts.player_name,
           ts.scoring_class, ts.is_active,
           ls.start_season,
           ts.current_season as end_season
    from this_season_data ts 
         join last_season_scd ls
         on ts.player_name = ls.player_name
    where ts.scoring_class = ls.scoring_class
    and ts.is_active = ls.is_active
),
-- changed é em relação à temporada 2021 apenas
-- o unnest combina o player_name com cada elemento do array, duplicando a cardinalidade da tabela
changed_records as (
    select ts.player_name,
           unnest(array[
            row(
                ls.scoring_class,
                ls.is_active,
                ls.start_season,
                ls.end_season
            )::scd_type,
            row(
                -- esse é o novo registro
                -- engloba tanto quem mudou quanto quem não estava na temporada passada
                ts.scoring_class,
                ts.is_active,
                ts.current_season,
                ts.current_season
            )::scd_type
           ]) as records
    from this_season_data ts 
         left join last_season_scd ls
         on ts.player_name = ls.player_name
    where (ts.scoring_class <> ls.scoring_class
    or ts.is_active <> ls.is_active)
),
unnested_changed_records as (
-- cte para achatar os dados da consulta anterior
    select
        player_name,
        (records::scd_type).scoring_class,
        (records::scd_type).is_active,
        (records::scd_type).start_season,
        (records::scd_type).end_season
    from changed_records 
),
new_records as (
    -- cte que considera apenas os registros novos (que apareceream em 2022)
    select 
        ts.player_name,
        ts.scoring_class,
        ts.is_active,
        ts.current_season as start_season,
        ts.current_season as end_season
    from this_season_data ts 
         left join last_season_scd ls
         on ts.player_name = ls.player_name
    where ls.player_name is null
)
/*
Todas as possibilidades estão contempladas aqui:
    1- dados históricos
    2- dados que não foram alterados
    2- dados que foram alterados
    3- novos dados
*/
-- union all elimina duplicação, então não há problemas ...
select * from historical_scd
union all
select * from unchanged_records
union all
select * from unnested_changed_records
union all
select * from new_records

create type scd_type as (
    scoring_class scoring_class,
    is_active boolean,
    start_season integer,
    end_season integer
)