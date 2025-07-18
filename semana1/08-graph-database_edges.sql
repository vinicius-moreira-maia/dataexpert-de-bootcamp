-- Faltou ele modelar a aresta 'plays_on' ...

-- plays_in (jogador em relação ao jogo/'game'/partida)
insert into edges
with deduped as (
    select *,
           row_number() over (partition by player_id, game_id) as row_num
    from game_details 
)
select
    player_id as subject_identifier,
    'player'::vertex_type as subject_type,
    game_id as object_identifier,
    'game'::vertex_type as object_type,
    'plays_in'::edge_type as edge_type,
    json_build_object(
        'start_position', start_position,
        'pts', pts,
        'team_id', team_id,
        'team_abbreviation', team_abbreviation
    ) as properties
from deduped
where row_num = 1;

/* Self join de jogadores com jogadores
2 situações:
    - jogadores rivais
    - jogadores do mesmo time

relacionamento entre o jogador e os jogadores que ele enfrentou*/

-- ENUM é ótimo para qualidade de dados!!!!
insert into edges
with deduped as (
    select *,
           row_number() over (partition by player_id, game_id) as row_num
    from game_details 
),
filtered as (
    select * from deduped
    where row_num = 1
), 
aggregated as (
    select 
    f1.player_id as subject_player_id, 
    f2.player_id as object_player_id,
    -- checando se é adversário ou se é do mesmo time
    case 
        when f1.team_abbreviation = f2.team_abbreviation then 'shares_team'::edge_type
        else 'plays_againts'::edge_type 
    end as edge_type,

    -- alguém pode ter o mesmo id e nomes diferentes, portanto é preciso usar o max nos nomes para retornar apenas 1 dos nomes de cada jogador (alguém pode ter mudado de nome ou sei lá)
    max(f1.player_name) as subject_player_name,
    max(f2.player_name) as object_player_name,
    count(1) as num_games,
    sum(f1.pts) as subject_points,
    sum(f2.pts) as object_points
from filtered f1 join filtered f2
    on f1.game_id = f2.game_id
    and f1.player_name <> f2.player_name

-- aqui eu evito o relacionamento bidirecional
-- se A se conecta com B eu não preciso da linha em que b se conecta com A    
where f1.player_name > f2.player_name
group by
    f1.player_id,
    f2.player_id,
    case 
        when f1.team_abbreviation = f2.team_abbreviation then 'shares_team'::edge_type
        else 'plays_againts'::edge_type 
    end
)
select
    subject_player_id as subject_identifier,
    'player'::vertex_type as subject_type,
    object_player_id as object_identifier,
    'player'::vertex_type as object_type,
    edge_type as edge_type,
    json_build_object(
        'num_games', num_games,
        'subject_points', subject_points,
        'object_points', object_points
    ) as properties
from aggregated;

