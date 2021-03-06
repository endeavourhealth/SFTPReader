
create or replace function log.get_all_batches
(
	_configuration_id varchar
)
returns setof refcursor
as $$
declare
	_batch_ids integer[];
begin

	select
		array_agg(b.batch_id) into _batch_ids
	from log.batch b
	where b.configuration_id = _configuration_id;

	return query
	select
	  * 
	from log.get_batches(_batch_ids);

end;
$$ language plpgsql;