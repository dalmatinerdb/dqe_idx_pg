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

BEGIN;
--DROP FUNCTION add_metric(acollection text, ametric bytea, abucket text, akey bytea);
CREATE FUNCTION add_metric(acollection text, ametric bytea, abucket text, akey bytea) RETURNS bigint AS
$$
DECLARE aid integer;
BEGIN
   INSERT INTO metrics (collection, metric, bucket, key) VALUES (acollection, ametric, abucket, akey) ON CONFLICT DO NOTHING RETURNING id INTO aid;
   IF NOT found THEN
     SELECT id FROM metrics INTO aid WHERE collection = acollection
          AND metric = ametric
          AND bucket = abucket
          AND key = akey;
     RETURN aid;
   ELSE
     RETURN aid;
   END IF;
END;
$$
LANGUAGE plpgsql;
COMMIT;


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
CREATE INDEX tags_idx_name ON TAGS (namespace, name);
CREATE INDEX tags_idx_value ON TAGS (value);

-- BEGIN;
-- --DROP FUNCTION add_tag(ametric_id bigint, anamespace text, aname text, avalue text);
-- CREATE FUNCTION add_tag(ametric_id bigint, anamespace text, aname text, avalue text) RETURNS VOID AS
-- $$
-- DECLARE aid integer;
-- BEGIN
--    INSERT INTO tags (metric_id, namespace, name, value)
--       VALUES (ametric_id, anamespace, aname, avalue)
--    ON CONFLICT ON CONSTRAINT tags_metric_id_namespace_name_key DO UPDATE SET value = avalue;
--    RETURN;
-- END;
-- $$
-- LANGUAGE plpgsql;
-- COMMIT;

BEGIN;
--DROP FUNCTION add_tag(ametric_id bigint, anamespace text, aname text, avalue text);
CREATE FUNCTION add_tag(ametric_id bigint, anamespace text, aname text, avalue text) RETURNS VOID AS
$$
DECLARE aid integer;
BEGIN
   INSERT INTO tags (metric_id, namespace, name, value)
      VALUES (ametric_id, anamespace, aname, avalue)
   ON CONFLICT DO NOTHING;
   RETURN;
END;
$$
LANGUAGE plpgsql;
COMMIT;


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
