BEGIN;
-- DROP FUNCTION add_metric(acollection text, ametric bytea, abucket text, akey bytea);
CREATE FUNCTION add_metric(acollection text, ametric bytea, abucket text, akey bytea) RETURNS bigint AS
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
COMMIT;

BEGIN;
CREATE FUNCTION add_tag(ametric_id bigint, anamespace text, aname text, avalue text) RETURNS VOID AS
$$
DECLARE aid integer;
BEGIN
    LOOP
        -- first try to update the key
        -- note that "a" must be unique
        PERFORM FROM tags WHERE metric_id = ametric_id
          AND namespace = anamespace
          AND name = aname
          AND value = avalue;
        IF found THEN
            RETURN;
        END IF;
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO tags (metric_id, namespace, name, value)
              VALUES (ametric_id, anamespace, aname, avalue);
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- do nothing, and loop to try the UPDATE again
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;
COMMIT;

BEGIN;
CREATE FUNCTION update_tag(ametric_id bigint, anamespace text, aname text, avalue text) RETURNS VOID AS
$$
DECLARE aid integer;
BEGIN
    LOOP
        -- first try to update the key
        -- note that "a" must be unique
        PERFORM FROM tags WHERE metric_id = ametric_id
          AND namespace = anamespace
          AND name = aname
          AND value = avalue;
        IF found THEN
            UPDATE tags SET value = avalue WHERE metric_id = ametric_id
          AND namespace = anamespace
          AND name = aname
          AND value = avalue;
        END IF;
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO tags (metric_id, namespace, name, value)
              VALUES (ametric_id, anamespace, aname, avalue);
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- do nothing, and loop to try the UPDATE again
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;
COMMIT;
