
CREATE OR REPLACE FUNCTION log.get_batch_splits
(
  _batch_split_ids integer[]
)
  RETURNS SETOF refcursor
    LANGUAGE 'plpgsql'
AS $BODY$
declare
	batch_split refcursor;
begin

	batch_split = 'batch_split';

	open batch_split for
		select
			b.batch_split_id,
			b.batch_id,
			b.local_relative_path,
			b.organisation_id,
			b.is_bulk,
			b.has_patient_data
		from log.batch_split b
		where b.batch_split_id in
		(
			select unnest(_batch_split_ids)
		);
	return next batch_split;

end;
$BODY$;

ALTER FUNCTION log.get_batch_splits(integer[])
    OWNER TO postgres;

GRANT EXECUTE ON FUNCTION log.get_batch_splits(integer[]) TO ddsui;

GRANT EXECUTE ON FUNCTION log.get_batch_splits(integer[]) TO postgres;

GRANT EXECUTE ON FUNCTION log.get_batch_splits(integer[]) TO PUBLIC;

GRANT EXECUTE ON FUNCTION log.get_batch_splits(integer[]) TO "sftp-reader";

