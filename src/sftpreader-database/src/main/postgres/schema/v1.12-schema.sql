

-- new columns on eds table
ALTER TABLE configuration.eds
ADD temp_directory varchar(255);

ALTER TABLE configuration.eds
ADD shared_storage_path varchar(255);

-- move data from old storage place to new
UPDATE configuration.eds
SET shared_storage_path = local_root_path_prefix
FROM configuration.configuration

-- drop old storage place column
ALTER TABLE configuration.configuration
DROP local_root_path_prefix;

-- drop this constraint as we now allow the sequence number to be set without a batch being complete
ALTER TABLE log.batch
DROP CONSTRAINT log_batch_iscomplete_completedate_sequencenumber_ck;