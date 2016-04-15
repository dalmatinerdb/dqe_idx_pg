-- CREATE DATABASE metric_metadata;
CREATE USER ddb WITH PASSWORD 'ddb';
CREATE DATABASE metric_metadata;
GRANT ALL PRIVILEGES ON metric_metadata TO ddb;
\connect metric_metadata;


DROP TABLE metrics CASCADE;
CREATE TABLE metrics (
    id          bigserial PRIMARY key,
    collection  varchar NOT NULL,
    metric      varchar NOT NULL,
    bucket      varchar NOT NULL,
    key         varchar NOT NULL
);

CREATE UNIQUE INDEX metrics_idx ON metrics (collection, metric, bucket, key);

DROP FUNCTION add_metric(acollection text, ametric text, abucket text, akey text);
CREATE FUNCTION add_metric(acollection text, ametric text, abucket text, akey text) RETURNS bigint AS
$$
DECLARE aid integer;
BEGIN
    LOOP
        -- first try to update the key
        -- note that "a" must be unique
        SELECT id FROM metrics INTO aid WHERE collection = acollection 
          AND metric = ametric 
          AND bucket = abucket
          AND key = akey;
        IF found THEN
            RETURN aid;
        END IF;
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO metrics (collection, metric, bucket, key) VALUES (acollection, ametric, abucket, akey) RETURNING id INTO aid;
            RETURN aid;
        EXCEPTION WHEN unique_violation THEN
            -- do nothing, and loop to try the UPDATE again
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;

DROP TABLE tags;
CREATE TABLE tags (
    metric_id bigserial REFERENCES metrics (id) ON DELETE CASCADE,
    name   text NOT NULL,
    value  text NOT NULL
);
CREATE UNIQUE INDEX tags_idx ON tags (metric_id, name, value);
CREATE UNIQUE INDEX tags_name_idx ON tags (metric_id, name);
CREATE INDEX tags_idx_name ON TAGS (name);
CREATE INDEX tags_idx_value ON TAGS (value);

DROP FUNCTION add_tag(ametric_id bigint, aname text, avalue text);
CREATE FUNCTION add_tag(ametric_id bigint, aname text, avalue text) RETURNS VOID AS
$$
DECLARE aid integer;
BEGIN
    LOOP
        -- first try to update the key
        -- note that "a" must be unique
        PERFORM FROM tags WHERE metric_id = ametric_id 
          AND name = aname
          AND value = avalue;
        IF found THEN
            RETURN;
        END IF;
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO tags (metric_id, name, value) VALUES (ametric_id, aname, avalue);
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- do nothing, and loop to try the UPDATE again
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;

DROP TABLE metric_elements;
CREATE TABLE metric_elements (
  metric_id bigserial REFERENCES metrics (id) ON DELETE CASCADE,
  position integer CHECK (position > 9),
  tag_metric text
);
CREATE UNIQUE INDEX metric_elements_idx ON metric_elements (metric_id, position, tag_metric);
