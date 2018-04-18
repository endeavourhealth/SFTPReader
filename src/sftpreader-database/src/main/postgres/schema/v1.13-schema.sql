-- Cerner 2.2 extracts can have multiple files of the same type in a batch
alter table log.batch_file
drop constraint log_batchfile_batchid_filetypeidentifier_uq;