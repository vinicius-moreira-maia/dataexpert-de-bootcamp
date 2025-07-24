-- incremental load

-- tipo auxiliar
create type scd_type2 as (
    quality_class performance_quality,
    is_active boolean,
    start_year integer,
    end_year integer
)

-- ainda há problemas de PK nesse insert ... e insert into NÃO faz da operação idempotente

-- insert into actors_history_scd
with ano_atual as (
	select 1971 as ano -- alterar aqui
), 
ultimo_ano_scd as (
    -- essa primeira cte traz a última partição dos dados históricos
	select * from actors_history_scd
	where current_year = (select ano from ano_atual) - 1
	and end_year = (select ano from ano_atual) - 1
), 
dados_historicos_scd as (
    -- essa cte traz os dados históricos anteriores à última partição
	select actor_name, quality_class, is_active, start_year, end_year
	from actors_history_scd
	where current_year = (select ano from ano_atual) - 1
	and end_year < (select ano from ano_atual) - 1
), 
dados_atuais as (
    -- dados atuais (da tabela acumulativa)
	select * from actors
	where current_year = (select ano from ano_atual)
), 
dados_nao_alterados as (
    -- join entre dados atuais e dados da última partição dos dados históricos
    -- aqui o ator teve filme nos 2 anos, então o ano de início continua sendo o anterior e o último ano é atualizado para o ano atual 
	select
		da.actor_name,
		da.quality_class,
		da.is_active,
		ua.start_year,
		da.current_year as end_year
	from dados_atuais da join ultimo_ano_scd ua
		on da.actor_name = ua.actor_name
	where
		da.quality_class = ua.quality_class
		and da.is_active = ua.is_active
), 
dados_alterados as (
    -- se algum dos dados observados forem alterados, guardar as duas versões do registro
    -- o unnest faz com que a cardinalidade dessa consulta seja duplicada
    -- 'records' nesse caso será uma coluna com objetos do tipo scd_type2
	select 
	da.actor_name,
	unnest(array[ -- registro antigo
	  row( ua.quality_class,
		   ua.is_active,
		   ua.start_year,
		   ua.end_year
	     )::scd_type2,
	  row( -- novo registro
		  da.quality_class,
		  da.is_active,
		  da.current_year,
		  da.current_year
	     )::scd_type2
	  ]) as records
	from dados_atuais da left join ultimo_ano_scd ua
		on da.actor_name = ua.actor_name
	where
		(da.quality_class <> ua.quality_class
		or da.is_active <> ua.is_active)	
), 
dados_alterados_flat as (
    -- aqui eu "quebro" os campos dos objetos do tipo scd_type2 cada um em uma coluna
	select
		actor_name,
		(records::scd_type2).quality_class,
		(records::scd_type2).is_active,
		(records::scd_type2).start_year,
		(records::scd_type2).end_year
	from dados_alterados
), 
novos_registros as (
    -- cte que considera apenas os registros novos (atores que começaram na data atual)
	select
		da.actor_name,
		da.quality_class,
		da.is_active,
		da.current_year as start_year,
		da.current_year as end_year
	from dados_atuais da left join ultimo_ano_scd ua
		on da.actor_name = ua.actor_name
	where
		ua.actor_name is null
)
select *, (select ano from ano_atual) from dados_historicos_scd -- massa de dados históricos
union all
select *, (select ano from ano_atual) from dados_nao_alterados
union all
select *, (select ano from ano_atual) from dados_alterados_flat
union all
select *, (select ano from ano_atual) from novos_registros