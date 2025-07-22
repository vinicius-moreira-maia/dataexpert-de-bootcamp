/*
O objetivo real por trás de tabelas fato é obter um dataset com o máximo poder analítico e que seja fácil e rápido de consultar.
*/

-- agora as "dimensões" da tabela fato podem facilmente ser usadas para trazer mais contexto
select t.*, gd.* from fact_game_details gd join teams t
on t.team_id = gd.dim_team_id

-- NWT (not_with_team)
-- jogadores de um time que nem ao jogo foram
-- 'bail_percentage' é a porcentagem de vezes que o cara nem pro jogo foi
select 
    dim_player_name,
    count(1) as num_games, 
    count(case when dim_not_with_team then 1 end) as bailed_num,
    cast(count(case when dim_not_with_team then 1 end) as real) / count(1) as bail_percentage 
from fact_game_details
group by 1
order by 4 desc