-- CREATE DATABASE metric_metadata;
CREATE USER ddb WITH PASSWORD 'ddb';
CREATE DATABASE metric_metadata OWNER ddb;
GRANT ALL ON DATABASE metric_metadata TO ddb;
\connect metric_metadata;

CREATE TABLE metrics (
    id          bigserial PRIMARY key,
    collection  varchar NOT NULL,
    metric      bytea NOT NULL,
    bucket      varchar NOT NULL,
    key         bytea NOT NULL
);
GRANT ALL On metrics TO ddb;
GRANT ALL On metrics_id_seq TO ddb;

CREATE UNIQUE INDEX metrics_idx_id ON metrics (id);
CREATE UNIQUE INDEX metrics_idx ON metrics (collection, metric, bucket, key);
CREATE UNIQUE INDEX metrics_idx_id_collection ON metrics (id, collection);
CREATE INDEX metrics_idx_collection ON metrics USING btree (collection);
CREATE INDEX metrics_idx_metric ON metrics USING btree (metric);

CREATE TABLE tags (
    metric_id bigserial REFERENCES metrics (id) ON DELETE CASCADE,
    collection  varchar NOT NULL,
    namespace   text NOT NULL,
    name        text NOT NULL,
    value       text NOT NULL,
    UNIQUE(metric_id, namespace, name)
);

GRANT ALL On tags TO ddb;

CREATE UNIQUE INDEX tags_idx ON tags (metric_id, namespace, name, value);
CREATE UNIQUE INDEX tags_name_idx ON tags (metric_id, namespace, name);

CREATE INDEX tags_idx_metric_id ON tags (metric_id);
CREATE INDEX tags_idx_namespace_name ON tags (namespace, name);
CREATE INDEX tags_idx_id_namespace ON tags (metric_id, namespace);
CREATE INDEX tags_idx_namespace_name_value ON tags (namespace, name, value);
CREATE INDEX tags_idx_value ON tags USING btree (value);
-- for lookups
CREATE INDEX tags_idx_collection ON tags USING btree (collection);
CREATE INDEX tags_idx_name ON tags USING btree (name);
CREATE INDEX tags_idx_namespace ON tags USING btree (namespace);
