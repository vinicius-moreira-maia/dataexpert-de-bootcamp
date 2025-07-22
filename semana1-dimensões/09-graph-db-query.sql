-- tudo que está no json é texto, então para agregar é preciso converter para inteiro
select 
    v.properties->>'player_name' as player_name,
    max((e.properties->>'pts')::integer)
from vertices v join edges e
    on e.subject_identifier = v.identifier
    and e.subject_type = v.type
group by 1
having 2 is not null
order by 2 desc
;

-- query que me dá os pontos por jogo de um jogador quando ele joga com outro jogador em específico
select 
    v.properties->>'player_name',
    e.object_identifier, -- id do vertex de outro jogador

    -- nº de jogos / nº de pontos (média)
    cast(v.properties->>'number_of_games' as real)/
    case 
        when cast(v.properties->>'total_points' as real) = 0 then 1 
        else cast(v.properties->>'total_points' as real)
    end as avg_points_per_game,
    e.properties->>'subject_points',
    e.properties->>'num_games'
from edges e join vertices v
	on v.identifier = e.object_identifier
    and v.type = e.subject_type
where e.object_type = 'player'::vertex_type;