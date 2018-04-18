--add new column to emis org map
ALTER TABLE configuration.emis_organisation_map
ADD start_date date;

--remove non-null consraint from notification table
ALTER TABLE log.notification_message
ALTER COLUMN outbound DROP NOT NULL;