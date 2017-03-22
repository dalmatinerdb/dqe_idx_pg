CREATE USER ddb WITH PASSWORD 'ddb';
CREATE DATABASE metric_metadata OWNER ddb;
GRANT ALL ON DATABASE metric_metadata TO ddb;
\connect metric_metadata;

CREATE EXTENSION hstore;

CREATE TABLE metrics (
    collection  text NOT NULL,
    metric      text[] NOT NULL,
    bucket      text NOT NULL,
    key         text[] NOT NULL,
    dimensions  hstore
);
GRANT ALL On metrics TO ddb;

-- Core index used to ensure that metrics are not overlapping.
-- It is also used by collections and metrics discovery.
CREATE UNIQUE INDEX ON metrics (collection, metric, bucket, key);

-- A helper index used to assist in tag namespaces and names discovery
CREATE INDEX ON metrics USING btree(collection, akeys(dimensions));
CREATE INDEX ON metrics USING btree(collection, metric, akeys(dimensions));

-- A main index used in dimensional lookups
CREATE INDEX ON metrics USING GIST (dimensions);
