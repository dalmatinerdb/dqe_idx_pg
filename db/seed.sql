--
-- Data for Name: metrics; Type: TABLE DATA; Schema: public; Owner: davidcromberge
--

COPY metrics (id, tag_bucket, tag_metric, bucket, metric_0, metric_1, metric_2, metric_3, metric_4, metric_5, metric_6, metric_7, metric_8, metric_9) FROM stdin;
1	553e7645d2a9d06502f49e0b	55	one.two.three.four.five.six.seven.eight.nine	zero	one	two	three	four	five	six	seven	eight	nine
2	553e7645d2a9d06502f49e0b	55	base.cpu	base	cpu	\N	\N	\N	\N	\N	\N	\N	\N
3	553e7645d2a9d06502f49e0b	55	base.memory	base	memory	\N	\N	\N	\N	\N	\N	\N	\N
4	553e7645d2a9d06502f49e0b	55	net.bytes	net	bytes	\N	\N	\N	\N	\N	\N	\N	\N
\.

--
-- Data for Name: metric_elements; Type: TABLE DATA; Schema: public; Owner: davidcromberge
--

COPY metric_elements (metric_id, "position", tag_metric) FROM stdin;
1	10	ten
1	11	eleven
\.
--
-- Data for Name: tags; Type: TABLE DATA; Schema: public; Owner: davidcromberge
--

COPY tags (metric_id, name, value) FROM stdin;
1	host	worker1
2	cpu	8
2	host	web1
3	host	web1
4	direction	in
4	iface	eth0
\.
