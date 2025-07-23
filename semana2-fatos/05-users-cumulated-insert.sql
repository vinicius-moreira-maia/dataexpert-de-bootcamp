-- 'events' é uma tabela com requisições a sites reais
-- o objetivo aqui é criar uma tabela acumulativa com os dias em que cada usuário esteve ativo

select * from events
select * from users_cumulated

select max(event_time), min(event_time) from events;
-- máximo 2023-01-31 23:51:51.685000
-- mínimo 2023-01-01 00:06:50.079000 (começando com '2022-12-31' é a seed query)

-- É preciso entender o que significa, nos dados, que um usuário esteve ativo. (interpretação!)

-- existem dados para cada dia do mês (tudo já foi adicionado)

-- tabela acumulativa

insert into users_cumulated
with yesterday as (
	select * from users_cumulated
	where current_date1 = date('2023-01-30')
), today as (
	-- essa aqui é uma forma bem arbitrária de determinar se o usuário está ativo ou não
	select
		cast(user_id as text) as user_id, -- essa conversão é para aderir ao schema da tabela criada
		date(cast(event_time as timestamp)) as date_active
	from events
	where date(cast(event_time as timestamp)) = date('2023-01-31')
		and user_id is not null -- o fato de possuir nulls aqui é problema de eng. de software
	group by user_id, date(cast(event_time as timestamp))
)
select 
	coalesce(t.user_id, y.user_id) as user_id,
	case
		-- se os dados de ontem forem null, incluir data de hoje no array
		when y.dates_active is null -- y.dates_active é o array da tabela criada
			then array[t.date_active] -- t.date_active é uma coluna vinda da cte
		-- se os dados de hoje forem null, trazer o histórico de ontem
		when t.date_active is null
			then y.dates_active
		-- se ambos não forem nulos, concatenar
		else
			-- para este caso, as datas mais recentes devem estar nos menores índices do array 
			array[t.date_active] || y.dates_active
	end as dates_active,

	/*
	Mesma coisa, se a data de hoje (today) for null, é preciso atualizar a data de hoje 
	a partir da data de ontem (+1 dia).
	*/
	coalesce(t.date_active, y.current_date1 + interval '1 day') as current_date1
from today t full outer join yesterday y
	on t.user_id = y.user_id;