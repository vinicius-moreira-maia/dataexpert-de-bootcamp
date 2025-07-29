with hosts as (
	-- aqui é a tabela acumulativa, onde eu tenho uma lista de datas em que o host recebeu alguma solicitação
	select 
		* 
	from hosts_cumulated 
	where current_date1 = date('2023-01-31')
), series as (
	-- aqui eu gero uma tabela contendo todas as datas sem repetição (para o período em que há registro de log)
	select 
		* 
	from generate_series(date('2023-01-02'), date('2023-01-31'), interval '1 day') as series_date
),placeholder_int as (
	select 
		-- isso me dá o nº de dias da diferença entre a data de cada registro e a data atual
		-- current_date1 - date(series_date),
	
		-- '@>' é o operador 'contém' para arrays
		-- "O array dates_active contém o array [series_date]?"
		case
			when dates_active @> array [date(series_date)]
				-- TRANSFORMANDO TODAS AS DATAS EM POTÊNCIAS DE 2
				-- 2^32, 2^31...2^5 (da mais recente para a mais antiga)
				-- a diferença entre current_date1 - series_date varia de 0 a 29
				-- fazendo 32 - diferença, o resultado passa a variar de 32 até 3
				-- o nº resultante é a posição do bit a ser ligado (da esq. p/ dir.)
				-- o pow(2, ...) "ativa" (transforma em 1) apenas o bit do resultado anterior
				-- no final de tudo o binário é convertido para bigint
				then cast(pow(2, 32 - (current_date1 - date(series_date))) as bigint)
			else 0 -- nenhum bit ativado
		end as placeholder_int_value,
		* 

	-- produto cartesiano (MUITAS linhas)
	-- a ideia é combinar cada registro de usuário com cada data da cte 'series'
	-- já que cada registro possui um par com cada data, a operação do select pode ser feita corretamente
	from hosts cross join series
)
select 
	host,

	-- quando eu somo esse valor, obtenho o único inteiro que representa toda a atividade do usuário
	-- convertendo essa soma para binário (32 bits), eu obtenho a representação de todo o histórico
	-- binários podem ser somados (onde 0 + 1 é 1 =))
	-- sum(placeholder_int_value),
	(cast(cast(sum(placeholder_int_value) as bigint) as bit(32))) as hosts_activity_datelist,

	-- bit_count conta quantos bits ativos em um n° binário
	bit_count(cast(cast(sum(placeholder_int_value) as bigint) as bit(32))) > 0 as dim_is_monthly_active,

	-- & compara 2 binários bit a bit e retorna 1 somente quando ambos os bits forem 1
	-- esses 1's representam uma máscara binária (são 7 ativos, representando APENAS a última semana)
	-- após essa comparação eu conto os bits e faço a expressão lógica
	bit_count(
		cast('11111110000000000000000000000000' as bit(32)) &
		cast(cast(sum(placeholder_int_value) as bigint) as bit(32))
	) > 0 as dim_is_weekly_active,

	-- se está ativo no dia mais recente
	bit_count(
		cast('10000000000000000000000000000000' as bit(32)) &
		cast(cast(sum(placeholder_int_value) as bigint) as bit(32))
	) > 0 as dim_is_daily_active
from placeholder_int
group by host