create table users_cumulated (
	user_id text,

	-- lista de datas do passado em que o usuário esteve ativo
	dates_active date[],

	-- data atual para o usuário
	current_date1 date, -- 'current_date' é palavra reservada
	primary key(user_id, current_date1)
);