
create or replace function management.get_emis_extract_dates
()
returns table
(
	extract_date varchar,
	ex1 integer,
	ex2 integer,
	ex3 integer,
	ex4 integer
)
as $$

	with extracts as
	(
		select 
			configuration_id as extract_id,
			substring(batch_identifier, 1, 10) as extract_date
		from log.batch b
	)
	select 
		substring(date_series::varchar, 1, 10) as extract_date,
		case when e1.extract_id is not null then 1 else 0 end as ex1,
		case when e2.extract_id is not null then 1 else 0 end as ex2,
		case when e3.extract_id is not null then 1 else 0 end as ex3,
		case when e4.extract_id is not null then 1 else 0 end as ex4
	from generate_series((select min(substring(batch_identifier, 1, 10)::date) from log.batch), now(), '1 day') date_series
	left outer join extracts e1 on substring(date_series::varchar, 1, 10) = e1.extract_date and e1.extract_id = 'EMIS001'
	left outer join extracts e2 on substring(date_series::varchar, 1, 10) = e2.extract_date and e2.extract_id = 'EMIS002'
	left outer join extracts e3 on substring(date_series::varchar, 1, 10) = e3.extract_date and e3.extract_id = 'EMIS003'
	left outer join extracts e4 on substring(date_series::varchar, 1, 10) = e4.extract_date and e4.extract_id = 'EMIS004'
	order by substring(date_series::varchar, 1, 10) desc;
	
$$ language sql;
