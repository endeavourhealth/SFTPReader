CREATE INDEX ix_dds_ui_helper
ON log.notification_message (
	batch_id, batch_split_id, timestamp, error_text
);