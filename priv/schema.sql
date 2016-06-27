-- CREATE DATABASE metric_metadata;
CREATE USER ddb WITH PASSWORD 'ddb';
CREATE DATABASE metric_metadata OWNER ddb;
GRANT ALL ON DATABASE metric_metadata TO ddb;
\connect metric_metadata;

CREATE TABLE metrics (
    id          bigserial PRIMARY key,
    collection  text NOT NULL,
    metric      text[] NOT NULL,
    bucket      text NOT NULL,
    key         text[] NOT NULL
);
GRANT ALL On metrics TO ddb;
GRANT ALL On metrics_id_seq TO ddb;

CREATE UNIQUE INDEX metrics_idx_id ON metrics (id);
CREATE UNIQUE INDEX metrics_idx ON metrics (collection, metric, bucket, key);
CREATE UNIQUE INDEX metrics_idx_id_collection ON metrics (id, collection);
CREATE INDEX metrics_idx_collection ON metrics USING btree (collection);
CREATE INDEX metrics_idx_metric ON metrics USING btree (metric);

CREATE TABLE dimensions (
    metric_id bigserial REFERENCES metrics (id) ON DELETE CASCADE,
    collection  text NOT NULL,
    namespace   text NOT NULL,
    name        text NOT NULL,
    value       text NOT NULL,
    UNIQUE(metric_id, namespace, name)
);

GRANT ALL On dimensions TO ddb;

CREATE UNIQUE INDEX dimensions_idx ON dimensions (metric_id, namespace, name, value);
CREATE UNIQUE INDEX dimensions_name_idx ON dimensions (metric_id, namespace, name);

CREATE INDEX dimensions_idx_metric_id ON dimensions (metric_id);
CREATE INDEX dimensions_idx_namespace_name ON dimensions (namespace, name);
CREATE INDEX dimensions_idx_id_namespace ON dimensions (metric_id, namespace);
CREATE INDEX dimensions_idx_namespace_name_value ON dimensions USING btree  (namespace, name, value);
CREATE INDEX dimensions_idx_value ON dimensions USING btree (value);
CREATE INDEX dimensions_idx_collection_namespace ON dimensions USING btree (collection, namespace);
-- for lookups
CREATE INDEX dimensions_idx_collection ON dimensions USING btree (collection);
CREATE INDEX dimensions_idx_name ON dimensions USING btree (name);
CREATE INDEX dimensions_idx_namespace ON dimensions USING btree (namespace);
CREATE INDEX dimensions_idx_collection_namespace_name ON dimensions USING btree (collection, namespace, name);
CREATE INDEX dimensions_idx_collection_namespace_name_value ON dimensions USING btree (collection, namespace, name, value);
