CREATE TABLE bucket_tags (
    bucket_name TEXT NOT NULL REFERENCES buckets(name) ON DELETE CASCADE,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (bucket_name, key)
);
