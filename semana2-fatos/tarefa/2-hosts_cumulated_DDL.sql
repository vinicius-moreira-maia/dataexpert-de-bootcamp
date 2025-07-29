create table hosts_cumulated (
	host text,
	-- lista de datas do passado em que o host foi acessado
	dates_active date[],
	current_date1 date,
	primary key(host, current_date1)
);