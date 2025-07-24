-- tabela que contém SCD's do tipo dois
create table actors_history_scd (
	actor_name text,
	
	-- colunas que terão as mudanças monitoradas (SCD's)
	quality_class performance_quality,
	is_active boolean,
	
	start_year integer,
	end_year integer,

	current_year integer,

	primary key(actor_name, start_year, end_year)
);