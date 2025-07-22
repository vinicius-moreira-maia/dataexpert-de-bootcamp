/*
Projeto de Tabela Acumulativa.
    - Em geral, trata-se de "jogar" os atributos temporais de uma entidade em uma coluna de tipo complexo.
      (muito comum em Big Data)
    - Essa coluna guarda cada versão dos elementos temporais (que mudam ao longo do tempo).
      (muito comum em Big Data)
    - Isso reduz a cardinalidade da tabela e torna as operações mais eficientes em sistemas distribuídos.
    - Isso também otimiza a geração de parquets, já que a "descompactação" dos tipos complexos em uma consulta produz um resultado
      com ordenação.
*/

-- struct para os elementos temporais em relação às atuações dos atores
CREATE TYPE film_stats AS (
    film text, 
    votes integer, 
    rating REAL, 
    filmid text,
    film_year integer -- coluna extra (não estava prevista no enunciado)
    );

create type performance_quality as enum 
('star', 'good', 'average', 'bad');

-- ddl da tabela acumulativa
create table actors (
    -- atributos fixos
    actor_id text,
    actor_name text,

    -- atributos temporais
    films film_stats[],
    
    -- analytics
    quality_class performance_quality,

    -- se está ativo na indústria (fazendo filme esse ano)
    is_active boolean,
    
    -- essa é a coluna necessária para a carga da tabela
    current_year INTEGER,

    primary key(actor_id, actor_name, current_year)
);