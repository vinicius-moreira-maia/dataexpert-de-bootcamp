-- Populando a tabela cumulativa com todos os anos disponíveis
DO $$
DECLARE
    ano_ontem INTEGER;
    ano_hoje INTEGER;
BEGIN
    FOR ano_ontem IN 1995..2021 LOOP
        ano_hoje := ano_ontem + 1;
        CALL atualiza_players(ano_ontem, ano_hoje);
    END LOOP;
END;
$$;

-- Isso aqui é uma gambiarra 
-- Não consegui incluir essa lógica na consulta de verificar se está ativo na consulta da tabela acumulativa.

alter table players add column is_active boolean;

update players set is_active = true
where (seasons[cardinality(seasons)]).season = current_season;

update players set is_active = false
where is_active is null;