--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: tags; Type: TABLE; Schema: public; Owner: davidcromberge; Tablespace: 
--

CREATE TABLE tags (
    tag_id bigint NOT NULL,
    tag_bucket character varying(40) NOT NULL,
    tag_name character varying(20) NOT NULL,
    tag_value character varying(20) NOT NULL,
    metric character varying(100) NOT NULL,
    bucket character varying(100) NOT NULL
);


ALTER TABLE tags OWNER TO davidcromberge;

--
-- Name: tags_tag_id_seq; Type: SEQUENCE; Schema: public; Owner: davidcromberge
--

CREATE SEQUENCE tags_tag_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE tags_tag_id_seq OWNER TO davidcromberge;

--
-- Name: tags_tag_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: davidcromberge
--

ALTER SEQUENCE tags_tag_id_seq OWNED BY tags.tag_id;


--
-- Name: tag_id; Type: DEFAULT; Schema: public; Owner: davidcromberge
--

ALTER TABLE ONLY tags ALTER COLUMN tag_id SET DEFAULT nextval('tags_tag_id_seq'::regclass);


--
-- Data for Name: tags; Type: TABLE DATA; Schema: public; Owner: davidcromberge
--

COPY tags (tag_id, tag_bucket, tag_name, tag_value, metric, bucket) FROM stdin;
1	553e7645d2a9d06502f49e0b	host	web1	base.cpu	55
2	553e7645d2a9d06502f49e0b	cpu	8	base.cpu	55
3	553e7645d2a9d06502f49e0b	host	web1	base.memory	55
4	55abedc02dc8284c13259592	iface	eth0	net.bytes	55
5	55abedc02dc8284c13259592	direction	in	net.bytes	55
6	55abedc02dc8284c13259592	direction	out	net.bytes	55
7	553e7645d2a9d06502f49e0b	host	web1	base.cpu	55
8	553e7645d2a9d06502f49e0b	cpu	8	base.cpu	55
9	553e7645d2a9d06502f49e0b	host	web1	base.memory	55
10	55abedc02dc8284c13259592	iface	eth0	net.bytes	55
11	55abedc02dc8284c13259592	direction	in	net.bytes	55
12	55abedc02dc8284c13259592	direction	out	net.bytes	55
\.


--
-- Name: tags_tag_id_seq; Type: SEQUENCE SET; Schema: public; Owner: davidcromberge
--

SELECT pg_catalog.setval('tags_tag_id_seq', 12, true);


--
-- Name: tags_pkey; Type: CONSTRAINT; Schema: public; Owner: davidcromberge; Tablespace: 
--

ALTER TABLE ONLY tags
    ADD CONSTRAINT tags_pkey PRIMARY KEY (tag_id);


--
-- Name: public; Type: ACL; Schema: -; Owner: davidcromberge
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM davidcromberge;
GRANT ALL ON SCHEMA public TO davidcromberge;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- PostgreSQL database dump complete
--

