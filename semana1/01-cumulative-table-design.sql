/* 
- Há alguns elementos temporais nessa tabela (player_seasons)
  (que variam de acordo com 'season', ou temporada)

- Se for feito um join com eles em ambiente distribuído (por exemplo entre
  essa dimensão e uma fato, usando 'season' como uma das chaves), 
  ocorrerá shuffle. Shuffle reduz o poder de compressão dos arquivos 
  de output (Parquet), pois ele altera a ordenação.

- Se eu criar outra tabela com uma linha para cada jogador com uma coluna contendo
  um tipo com todos seus elementos temporais isso evitará shuffle. Pois
  a cardinalidade está sendo bastante reduzida. (1 linha para cada jogador)

- Reduzindo a cardinalidade essa tabela será reduzida, aumentando as chances de ocorrer broadcast join, onde cópias
  da tabela menor são enviadas para cada nó do cluster, eliminando a necessidade de fazer shuffle.
*/

-- Criando um "struct" para conter os elementos temporais. (que variam ao longo do tempo)
-- É basicamente um tipo de dado composto (vários tipos).
 CREATE TYPE season_stats AS (season Integer, pts REAL, ast REAL, reb REAL, weight INTEGER);

-- Enum cria uma lista de constantes.
-- Esse tipo aceita apenas esses 4 valores.
create type scoring_class as enum ('star', 'good', 'average', 'bad');

-- Nova tabela para ser a tabela de cardinalidade reduzida.
 CREATE TABLE players (
     -- valores que não mudam ao longo do tempo
     player_name TEXT,
     height TEXT,
     college TEXT,
     country TEXT,
     draft_year TEXT,
     draft_round TEXT,
     draft_number TEXT,

     -- coluna com elementos temporais
     seasons season_stats[],

     /*
     Essa é uma tabela cumulativa, então eu preciso
     do estado atual do elemento temporal principal
     para fazer os full outer joins. (Cumulative Table Design)

     O valor dessa coluna será sempre o último valor da temporada ('season').
     */
     current_season INTEGER,

     -- Colunas mais voltadas para analytics (métricas)
     scoring_class scoring_class, -- Baseada no tipo de dado criado (ENUM)
     years_since_last_season integer,

    -- PK composta.
     PRIMARY KEY (player_name, current_season)
 );

-- Criando a lógica da tabela cumulativa.

select min(season) from player_seasons; -- 1996 (primeira temporada)

/*
'Seed query' é quando o valor de ontem é null (resultado da primeira cte).

Processo:
1 - Full outer join dos dois data sets (ontem e hoje)
    (quando o resultado de ontem for null, trata-se da consulta semente,
    ou 'seed query', que é a primeira carga)

    -> 1995 (null) e 1996 (primeiro ano que teve temporada)

    -> Os únicos parâmetros que devem ser alterados para realizar as cargas
       são 'current_season' e 'season', das CTE's.  

2 - Coalesce id's e dimensões que não mudam.
3 - Combinar arrays e valores que mudam
*/

insert into players
with ontem as (
    select * from players
    where current_season = 2000
 ), hoje as (
    select * from player_seasons
    where season = 2001
 )
 select
    -- coalesce retorna o primeiro elemento não nulo, então se o valor de hoje for nulo o de ontem será retornado
    coalesce(h.player_name, o.player_name) as player_name,
    coalesce(h.height, o.height) as height,
    coalesce(h.college, o.college) as college,
    coalesce(h.country, o.country) as country,
    coalesce(h.draft_year, o.draft_year) as draft_year,
    coalesce(h.draft_round, o.draft_round) as draft_round,
    coalesce(h.draft_number, o.draft_number) as draft_number,
    
    -- isso é para ir incluindo ps valores do array
    -- row() gera uma tupla, e estou a convertendo para o tipo criado (season_stats)
    case when o.seasons is null -- Se ontem não houver registro, criar o array inicial com 1 valor.
        THEN array[row(
            h.season,
            h.pts,
            h.ast,
            h.reb,
            h.weight
        )::season_stats]
    when h.season is not null then   -- Se hoje não for null e ontem também não, concatenar ontem e hoje.
        o.seasons || array[row(
            h.season,
            h.pts,
            h.ast,
            h.reb,
            h.weight
           )::season_stats]

    -- Se ontem não for null mas hoje for, replicar o histórico.
    -- Isso evitar adicionar nulls ao array.
    else o.seasons
    end as season_stats,

    -- Current Season (a temporada atual do jogador precisa estar atualizada).
    -- Isso significa ter uma referência para os "dados de hoje" sempre atualizada.
    /*
    Lembrar que a tabela player_seasons contem os dados de "hoje", e a tabela
    players é a tabela acumulativa que está sendo atualizada.
    */

    -- Se um jogador não jogou na temporada atual mas jogou na passada, a referência para a atual é atualizada com + 1.
    -- current_season é a temporada atual, independente de jogador
    coalesce(h.season, o.current_season + 1) as current_season,

    -- Colunas de métricas
    -- scoring_class
    case
        when h.season is not null then -- se ele jogou essa temporada
            case 
                when h.pts > 20 then 'star'
                when h.pts > 15 then 'good'
                when h.pts > 10 then 'average'
                else 'bad'
            end::scoring_class -- cast para ENUM criado
        else o.scoring_class -- senão jogou, trazer histórico
    end as scoring_class,

    -- years_since_last_season
    -- O valor inicial SEMPRE será 0, pois durante o backfill isso vai ocorrer.
    -- Basicamente, enquanto o jogador não estiver na temporada atual a adição continuará e se ele estiver na temporada atual (que conta como o 'hoje') esse valor será zerado.
    case 
        -- significa que ele está na temporada atual
        when h.season is not null then 0

        -- se for null, usar a season de 'ontem' + 1
        -- 'ontem' significa apenas dados do passado
        else o.years_since_last_season + 1
    end as years_since_last_season

 from hoje h full outer join ontem o
 on h.player_name = o.player_name;

-- Com a tabela acumulativa corretamente populada e atualizada facilmente eu posso "transformá-la" novamente na tabela 'players_season', que é uma tabela achatada (flat).

/*
Na cte eu faço o UNNEST do array que contém os elementos do tipo de dado que foi criado.
Após isso eu utilizo ela e "explodo" os elementos do tipo de dado.
Notar que os valores desse tipo já possuem nomes pois foram definidos assim.
*/

-- Sempre que eu fizer o UNNEST desses dados temporais a tabela estará ordenada! =)
-- Com isso não preciso me preocupar com o Spark alterar a ordenação ao fazer Join.
with unnested as (
    select player_name,
		   current_season,
	       scoring_class,
	       years_since_last_season,

           -- o unnest separa cada elemento do array em uma linha diferente
           unnest(seasons)::season_stats as season_stats
    from 
           players
    where 
           current_season = 2001 and player_name = 'Michael Jordan'
)
select player_name,
	   current_season,
	   scoring_class,
	   years_since_last_season,
       (season_stats::season_stats).* -- dados do struct como colunas
from unnested;

-- jogador que teve a melhor evolução em relação a primeira e a última temporada que ele jogou
-- cardinality retorna o nº de elementos em um array/estrutura
select 
    player_name,
    seasons[1] as first_season, -- acessando o struct da primeira temporada
    seasons[cardinality(seasons)] as latest_season -- acessando o struct da última temporada
from players
where current_season = 2001;

-- continuação da consulta anterior
-- é preciso fazer o cast antes de acessar o valor da struct
select 
    player_name,
    (seasons[1]::season_stats).pts as first_season,
    (seasons[cardinality(seasons)]::season_stats).pts as latest_season
from players
where current_season = 2001;

-- continuação...
-- essa divisão me dá a razão de crescimento no desempenho (olhando apenas para os pontos), ou de queda
-- ->  ESSA CONSULTA É BEM RÁPIDA! (Com esse design de tabela acumulativa eu evito JOIN e GROUP BY !! ^^)
select 
    player_name,
    (seasons[cardinality(seasons)]::season_stats).pts /
    case -- case pra evitar erro de divisão por zero
        when (seasons[1]::season_stats).pts = 0 then 1 
    else (seasons[1]::season_stats).pts
	end
from players
where current_season = 2001
order by 2 desc; -- essa é a única parte que pode causar shuffle