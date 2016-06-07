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

CREATE UNIQUE INDEX metrics_idx ON metrics (collection, metric, bucket, key);

CREATE TABLE tags (
    metric_id bigserial REFERENCES metrics (id) ON DELETE CASCADE,
    namespace   text NOT NULL,
    name        text NOT NULL,
    value       text NOT NULL,
    UNIQUE(metric_id, namespace, name)
);

GRANT ALL On tags TO ddb;

CREATE UNIQUE INDEX tags_idx ON tags (metric_id, namespace, name, value);
CREATE UNIQUE INDEX tags_name_idx ON tags (metric_id, namespace, name);
CREATE INDEX tags_idx_metric_id ON tags (metric_id);
CREATE INDEX tags_idx_name ON tags (namespace, name);
CREATE INDEX tags_idx_namespace_name_value ON tags (namespace, name, value);
CREATE INDEX tags_idx_value ON tags (value);


-- List all bucket
-- SELECT DISTINCT(bucket) FROM metrics;

-- List all metrics in a bucket
-- SELECT DISTINCT(metric) FROM metrics WHERE bucket = 'bucket';

-- List all names/namepaces for a metric
-- SELECT DISTINCT(namespace) FROM tags LEFT JOIN metrics ON tags.metric_id = metrics.id WHERE metrics.collection = 'bucket' AND metrics.metric = 'metric';

-- List all names for a metric/namespace
-- SELECT DISTINCT(name) FROM tags LEFT JOIN metrics ON tags.metric_id = metrics.id WHERE metrics.collection = 'bucket' AND metrics.metric = 'metric' AND tags.namespace = 'ddb';

-- List all names/namepaces for a metric
-- SELECT DISTINCT(namespace, name) FROM tags LEFT JOIN metrics ON tags.metric_id = metrics.id WHERE metrics.collection = 'bucket' AND metrics.metric = 'metric';

-- List all values for a name/namespace
-- SELECT DISTINCT(value) FROM tags LEFT JOIN metrics ON tags.metric_id = metrics.id WHERE metrics.collection = 'bucket' AND metrics.metric = 'metric' AND tags.namespace = '' AND  tags.name = 'what';
