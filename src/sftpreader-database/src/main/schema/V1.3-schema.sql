
create table configuration.eds
(
	single_row_lock boolean,
	eds_url varchar(1000) not null,
	software_content_type varchar(100) not null,
	software_version varchar(100) not null,
	use_keycloak boolean not null,
	keycloak_token_uri varchar(500) null,
	keycloak_realm varchar(100) null,
	keycloak_username varchar(100) null,
	keycloak_password varchar(100) null,
	keycloak_clientid varchar(100) null,

	constraint configuration_eds_singlerowlock_pk primary key (single_row_lock),
	constraint configuration_eds_singlerowlock_ck check (single_row_lock = true),
	constraint configuration_eds_edsurl_ck check (char_length(trim(eds_url)) > 0),
	constraint configuration_eds_softwarecontenttype_ck check (char_length(trim(software_content_type)) > 0),
	constraint configuration_eds_softwareversion_ck check (char_length(trim(software_version)) > 0),
	constraint configuration_eds_keycloakfields_ck check ((not use_keycloak) or (keycloak_token_uri is not null and keycloak_realm is not null and keycloak_username is not null and keycloak_password is not null and keycloak_clientid is not null)),
	constraint configuration_eds_keycloaktokenuri_ck check (keycloak_token_uri is null or (char_length(trim(keycloak_token_uri)) > 0)),
	constraint configuration_eds_keycloakrealm_ck check (keycloak_realm is null or (char_length(trim(keycloak_realm)) > 0)),
	constraint configuration_eds_keycloakusername_ck check (keycloak_username is null or (char_length(trim(keycloak_username)) > 0)),
	constraint configuration_eds_keycloakpassword_ck check (keycloak_password is null or (char_length(trim(keycloak_password)) > 0)),
	constraint configuration_eds_keycloakclientid_ck check (keycloak_clientid is null or (char_length(trim(keycloak_clientid)) > 0))
);

insert into configuration.eds
(
	single_row_lock,
	eds_url,
	software_content_type,
	software_version,
	use_keycloak,
	keycloak_token_uri,
	keycloak_realm,
	keycloak_username,
	keycloak_password,
	keycloak_clientid
)
select
	True,
	eds_url,
	envelope_content_type,
	software_version,
	use_keycloak,
	keycloak_token_uri,
	keycloak_realm,
	keycloak_username,
	keycloak_password,
	keycloak_clientid
from configuration.configuration_eds;

drop table configuration.configuration_eds;
