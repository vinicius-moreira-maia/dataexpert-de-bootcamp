-- generate_series(start, stop, step) gera linhas a partir do range fornecido
-- select * from generate_series(date('2023-01-02'), date('2023-01-31'), interval '1 day')

-- select * from users_cumulated where user_id like '439578%'

/*
	O objetivo é verificar se a data em 'series_date' está no array. Caso esteja,
	criar um número binário de 32 bits onde cada bit representa um dia entre 
	2023-01-02 e 2023-01-31, e onde o bit está ligado (1) se o usuário esteve 
	ativo naquele dia, ou desligado (0) se não esteve.

	Essa é uma forma MUITO mais eficiente do que usar array de datas, afinal, 
	lida com binários!
*/

with users as (
	-- aqui é a tabela acumulativa, onde eu tenho uma lista de datas em que o usuário esteve ativo
	select 
		* 
	from users_cumulated 
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
				-- 2^32, 2^31...2^5 (da mais recebte para a mais antiga)
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
	from users cross join series
	--where user_id = '439578290726747300'
)
select 
	user_id,

	-- quando eu somo esse valor, obtenho o único inteiro que representa toda a atividade do usuário
	-- convertendo essa soma para binário (32 bits), eu obtenho a representação de todo o histórico
	-- binários podem ser somados (onde 0 + 1 é 1 =))
	sum(placeholder_int_value),
	cast(cast(sum(placeholder_int_value) as bigint) as bit(32)),

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
group by user_id



