-- checando se há nulos (não há)
-- select host, event_time from events
-- where host is null

-- eu poderia ter criado um array com structs para saber, também, a quantidade de requisições de cada host em cada dia

insert into hosts_cumulated
with yesterday as (
	select * from hosts_cumulated
	where current_date1 = date('2023-01-13')
), today as (
	select
		host,

		/*
		Seria bem relevante incluir essa métrica dentro do array ... 
		*/
		-- count(host) as daily_requests, 
		
		date(cast(event_time as timestamp)) as date_active
	from events
	where date(cast(event_time as timestamp)) = date('2023-01-14')
	group by host, date(cast(event_time as timestamp))
)
select 
	coalesce(t.host, y.host) as host,
	-- t.daily_requests,
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
	on t.host = y.host;