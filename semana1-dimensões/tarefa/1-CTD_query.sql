-- conhecendo o range da coluna que será usada para os "dados de hoje X dados de ontem"
select min(year) from actor_films; -- 1970
select max(year) from actor_films; -- 2021

-- consulta semente (seed query) -> 1969 e 1970

-- 1 -> full outer join de "ontem" com "hoje"
-- 2 -> coalesce de id's e atributos que não mudam
-- 3 -> combinação dos dos dados temporais de ontem com os de hoje (case statement com os arrays)

insert into actors
with ontem as (
    select * from actors
    where current_year = 1974
), hoje as (
    select * from actor_films
    where year = 1975
), ator_filme as (
	select
    	-- se os valores de hoje não forem nulos, retorne-os, senão retorne os de ontem
    	coalesce(h.actorid, o.actor_id) as actor_id,
    	coalesce(h.actor, o.actor_name) as actor_name,

    	case 
        	-- se ontem não houver registros (mas hoje sim)
        	when o.films is null
            	then array[row(
                	h.film, 
                	h.votes, 
                	h.rating, 
                	h.filmid,
					h.year
            	)::film_stats]
        	-- se hoje E ontem conter registros
        	-- concatenar os 2
        	when h.year is not null
            	then o.films || array[row(
                	h.film, 
                	h.votes, 
                	h.rating, 
                	h.filmid,
					h.year
            	)::film_stats]
        	-- se ontem tiver registros e hoje não, replicar o histórico (evita adicionar nulls ao array)
        	else o.films
    	end as films,

        -- Se um ator não esteve em filmes um ano anterior ao ano atual, a referência para o ano atual é atualizada (+1)
        coalesce(h.year, o.current_year + 1) as current_year

	from hoje h full outer join ontem o 
     	on o.actor_name = h.actor
), ator_filme_agg as (
	-- Essa cte serve para agregar os dados dos atores em um mesmo ano (pois ele pode ter atuado em mais de um filme por ano). 
	-- Também serve para calcular a média de 'rating', aproveitando o agrupamento já criado.
	-- Também serve para checar se o ator está ativo no ano atual.
	 select
        actor_id,
        actor_name,
        
		/*
			'array_agg()' estava agregando arrays, e não concatenando, ou seja, estava criando arrays de arrays. Isso estava causando erro na inserção.
			Já 'array_cat(array_agg(films), '{}')' achata o array multidimensional em um unidimensional.	
		*/
		-- array_agg(films) as films,
		array_cat(array_agg(films), '{}') as films,
        
		
		avg((films[cardinality(films)]::film_stats).rating) as avg_rating,
		current_year,
		case 
			when max((films[cardinality(films)]::film_stats).film_year) = current_year
				then true
			else false
		end as is_active
    from ator_filme
    group by actor_id, actor_name, current_year
), ator_filme_quality_class as (
	-- cte para criar a coluna que classifica os atores segundo a média de suas notas de atuação
	select 
		*,
		case 
			when avg_rating > 8 then 'star'::performance_quality
			when avg_rating > 7 and avg_rating <= 8 then 'good'::performance_quality
			when avg_rating > 6 and avg_rating <= 7 then 'average'::performance_quality
			else 'bad'::performance_quality
		end as quality_class
	from ator_filme_agg
)
select 
	actor_id,
	actor_name,
	films,
	quality_class,
	is_active,
	current_year
from ator_filme_quality_class
;