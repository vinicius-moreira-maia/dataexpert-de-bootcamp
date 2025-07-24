-- full load

insert into actors_history_scd
with estado_anterior as (
	select
		actor_name,
		current_year,
		quality_class,
		is_active,
		lag(quality_class) over (partition by actor_name order by current_year) as previous_quality_class,
    	lag(is_active) over (partition by actor_name order by current_year) as previous_is_active
	from actors
	where current_year <= 1970
), indicacao_mudanca as (
	select
		*,
		case
			when quality_class <> previous_quality_class then 1
			when is_active <> previous_is_active then 1
			else 0
		end as indicador_mudanca
	from estado_anterior
), somatorio_mudancas as (
	select
		*,
		sum(indicador_mudanca) over(partition by actor_name order by current_year) as quantidade_mudancas
	from indicacao_mudanca
)
select
	actor_name,
	quality_class,
	is_active,
	min(current_year) as start_year,
	max(current_year) as end_year,
	1970 as current_year
from somatorio_mudancas
group by actor_name, quality_class, is_active, quantidade_mudancas
order by actor_name, start_year;