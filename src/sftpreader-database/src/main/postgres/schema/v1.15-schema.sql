alter table log.batch_file
add is_deleted boolean;

alter table log.batch
drop constraint log_batch_configurationid_batchidentifier_uq;

CREATE INDEX ix_batch_configuration_identifer on log.batch (configuration_id, batch_identifier);

