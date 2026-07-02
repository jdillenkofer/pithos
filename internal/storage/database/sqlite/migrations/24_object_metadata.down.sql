DROP TABLE object_user_metadata;

ALTER TABLE objects DROP COLUMN website_redirect_location;
ALTER TABLE objects DROP COLUMN expires;
ALTER TABLE objects DROP COLUMN content_language;
ALTER TABLE objects DROP COLUMN content_encoding;
ALTER TABLE objects DROP COLUMN content_disposition;
ALTER TABLE objects DROP COLUMN cache_control;
