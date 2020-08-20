delimiter $$

drop procedure if exists reset_all_content;

$$

create procedure reset_all_content ()
begin

  -- empty all tables that are written to as part of normal processing
	delete from unknown_file;
	delete from notification_message;
	delete from batch_split;
	delete from batch_file;
	delete from batch;
	delete from emis_organisation_map;
	delete from tpp_organisation_gms_registration_map;
	delete from tpp_organisation_map;

  -- drop and recreate the hashes DB
  DROP DATABASE IF EXISTS sftp_reader_hashes;
  CREATE DATABASE sftp_reader_hashes;

end

$$

delimiter ;
