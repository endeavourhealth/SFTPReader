
create or replace function log.set_file_as_downloaded
(
	_batch_file_id integer
)
returns void
as $$
begin

	update log.batch_file
	set
		is_downloaded = true,
		download_date = date_trunc('second', now()::timestamp)
	where batch_file_id = _batch_file_id;

end;
$$ language plpgsql;

