DROP TABLE metrics CASCADE;
DROP FUNCTION add_metric(acollection text, ametric text, abucket text, akey text);
DROP TABLE tags;
DROP FUNCTION add_tag(ametric_id bigint, anamespace text, aname text, avalue text);
DROP TABLE metric_elements;
