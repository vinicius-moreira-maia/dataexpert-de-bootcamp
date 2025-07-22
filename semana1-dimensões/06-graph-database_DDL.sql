/*
Modelagem de Dados em Grafos.
-> Modelagem agnóstica! Apeas o conteúdo dos enums
   são mais específicos.
*/

-- os únicos tipos de nós permitidos nessa modelagem
-- jogador, time e jogo
create type vertex_type
    as enum('player', 'team', 'game');

create table vertices (
    identifier TEXT,
    type vertex_type,
    properties JSON,
    primary key(identifier, type)
);

-- os únicos tipos de relacionamento permitidos
create type edge_type 
    as enum('plays_against', -- joga contra
            'shares_team', -- joga com
            'plays_in', -- joga em um jogo
            'plays_on' -- joga em um time
            );

CREATE TABLE edges (
    subject_identifier TEXT,
    subject_type vertex_type,
    object_identifier TEXT,
    object_type vertex_type,
    edge_type edge_type,
    properties JSON,
    PRIMARY KEY (subject_identifier,
                subject_type,
                object_identifier,
                object_type,
                edge_type)
)