-- O grão da tabela fato é o que identifica a que cada registro se refere (uma transação, um processo, um evento, etc.)
-- Toda tabela fato deve possuir um grão bem definido.

-- query muito comum em dados de log para identificar duplicatas
-- "game_id, team_id, player_id" como a combinação que identifica unicamente um registro, pois cada linha se refere às estatísticas de apenas um jogador em apenas um jogo jogando em apenas um time
-- a tabela 'game_details' é BEM desnormalizada
select 
    game_id, team_id, player_id, count(1)
from game_details
group by 1, 2, 3
having count(1) > 1

/*
1- Criar um filtro para eliminar duplicatas.
2- Tudo que eu posso fazer join de forma barata não deve ir para fato. (no caso, as colunas relacionadas aos times, pois não há muitos times na NBA)

-> Tudo que for fácil de derivar não deve ir para a fato (é desperdício).
-> Atributos descritivos PODEM estar em tabela fato sim! =)
-> What (o que) e When (quando) são atributos inerentes ao fato
-> Who, Where e How estão mais no campo das chaves para outras tabelas
*/
insert into fact_game_details
with deduped as (
    select 
        g.game_date_est, -- data do jogo
        g.season,
        g.home_team_id,
        gd.*, 
        row_number() over(partition by gd.game_id, gd.team_id, gd.player_id order by g.game_date_est) as row_num
    
    -- o join com 'games' é para trazer o 'when' (quando)
    from game_details gd join games g 
        on gd.game_id = g.game_id
    -- where g.game_date_est = '2016-10-04'
)
select 
    game_date_est as dim_game_date, -- data do jogo
    season as dim_season,
    team_id as dim_team_id,
    player_id as dim_player_id,
    player_name as dim_player_name,
    start_position as dim_start_position,
    team_id = home_team_id as dim_is_playing_at_home,

    -- Position() é função de string, aqui retorna 1 caso a sigla exista (se existir é no começo mesmo), e 0 caso não exista devido ao Coalesce(). O retorno de coalesce() é comparado com 0 no teste lógico (se é maior ou não).
    -- Ter esses indicadores em fatos é melhor que manter os comentários.
    coalesce(position('DNP' in comment), 0) > 0 as dim_not_play,
    coalesce(position('DND' in comment), 0) > 0 as dim_not_dress,
    coalesce(position('NWT' in comment), 0) > 0 as dim_not_with_team,

    -- Antes a coluna 'min' era apenas texto, e mal formatado, agora a coluna é propícia para fazer análise (Engenheiros de Dados devem entregar dados de qualidade e analisáveis)
    cast(split_part(min, ':', 1) as real) +
    cast(split_part(min, ':', 2) as real) / 60
    as m_minutes,
    fgm as m_fgm,
    fga as m_fga,
    fg3m as m_fg3m,
    fg3a as m_fg3a,
    ftm as m_ftm,
    fta as m_fta,
    oreb as m_oreb,
    dreb as m_dreb,
    reb as m_reb,
    ast as m_ast,
    stl as m_stl,
    blk as m_blk,
    "TO" as m_turnovers,
    pf as m_pf,
    pts as m_pts,
    plus_minus as m_plus_minus
from deduped 
where row_num = 1