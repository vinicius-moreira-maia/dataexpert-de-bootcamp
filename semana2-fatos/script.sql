-- criação da tabela fato reduzida (minimização de shuffle)
create table array_metrics (
    user_id numeric,
    month_start date,
    metric_name text,
    metric_array real[]
    primary key(user_id, month_start, metric_name)
)

-- notar que quase tudo que foi visto em modelagem de dados aqui passa pelo design de tabelas acumulativas
insert into array_metrics
with daily_aggregate as (
    select
        user_id,
        date(event_time) as current_date1,
        count(1) as num_site_hits 
    from events
    where date(event_time) = date('2023-01-01') -- alterar aqui
    and user_id is not null
    group by user_id, date(event_time)
), yesterday_array as (
    select *
    from array_metrics
    where month_start = date('2023-01-01')
)
select 
        coalesce(da.user_id, ya.user_id) as user_id,
        coalesce(ya.month_start, date_trunc('month', da.current_date1)) as month_start,
        'site_hits' as metric_name,
        case 
            -- se o array de ontem não for nulo, concatenar com o array de hoje
            -- significa que o usuário existe
            when ya.metric_array is not null then
                ya.metric_array || array[coalesce(da.num_site_hits, 0)]

            -- se o array de ontem for nulo, trazer o histórico
            when ya.metric_array is null then 
                /*
                Se eu estiver no sétimo dia e aparecer um novo usuário, será 7 - 1 (1 do month start), ou seja, 6 zeros no array.
                Isso é devido ao fato de que todos os arrays aqui devem possuir o mesmo tamanho.
                Ao contrário do datelist, aqui o valor mais da esquerda é o valor mais antigo mesmo.
                'array_fill' cria um array.
                O coalesce serve caso current_date1 ou month_start forem null, 0 será retornado e o 'fill' do array não será feito. (aqui no caso ´é o 1º dia do mês) 
                */
                array_fill(0, array[coalesce(current_date1 - date(date_trunc('month', current_date1)), 0)]) || array[coalesce(da.num_site_hits, 0)]
        end as metric_array
from daily_aggregate da full outer join yesterday_array ya on da.user_id = ya.user_id
-- se der conflito de chave, atualizar apenas o metric array
-- esse on conflict fará com que o comando seja idempotente (usar insert into é problemático nesse sentido ...)
on conflict(user_id, month_start, metric_name)
do update set metric_array = excluded.metric_array

-- essa consulta me dá o agregado dos 3 primeiros dias, separados por linhas
with agg as (
    select 
        metric_name,
        month_start,
        array[sum(metric_array[1]),
              sum(metric_array[2]),
              sum(metric_array[3])] as summed_array
    from array_metrics
    group by metric_name, month_start
)
select 
    metric_name, 
    month_start + cast(cast(index - 1 as text) || 'day' as interval),
    elem as value
from agg cross join unnest(agg.summed_array) 
        with ordinality as a(elem, index)