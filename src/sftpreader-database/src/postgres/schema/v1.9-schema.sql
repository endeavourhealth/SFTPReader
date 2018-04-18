-- moving two columns to different table
ALTER TABLE configuration.configuration
ADD software_content_type character varying(100) NOT NULL DEFAULT '';

ALTER TABLE configuration.configuration
ADD software_version character varying(100) NOT NULL DEFAULT '';

UPDATE configuration.configuration
SET
	software_content_type = eds.software_content_type,
	software_version = eds.software_version
FROM configuration.eds;

ALTER TABLE configuration.eds
DROP COLUMN software_content_type;

ALTER TABLE configuration.eds
DROP COLUMN software_version;

-- slack table is no longer used
DROP TABLE configuration.slack;


