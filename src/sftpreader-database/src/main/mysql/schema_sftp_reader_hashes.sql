-- Scripts for SD-70 TPP SRCode Re-bulks Need Better Handling
create database sftp_reader_hashes;
use sftp_reader_hashes;

DROP TABLE IF EXISTS file_record_hash;

CREATE TABLE file_record_hash (
  record_id varchar(100) NOT NULL,
  record_hash char(128) DEFAULT NULL,
  dt_last_updated datetime NOT NULL,
	CONSTRAINT file_record_hash_record_id_pk PRIMARY KEY (record_id),
	INDEX ix_id_hash  (record_id, record_hash),
    INDEX ix_id_date  (record_id, dt_last_updated)
);

 insert into config.config value ('global', 'db_sftp_reader_hashes', '{
  	"maximumPoolSize": 5,
  	"url" : "<>",
  	"username" : "root", "password" : "<>",
  	"class": "com.mysql.cj.jdbc.MysqlDataSource"
 }');

