CREATE TABLE public.staging_users
(
	created_at varchar(256),
	id int,
	followers_count int,
	follows_count int,
	media_count int,
	impressions int,
	reach int,
	follower_count int,
	email_contacts int,
	phone_call_clicks int,
	text_message_clicks int,
	get_directions_clicks int,
	website_clicks int,
	profile_views int
);


CREATE TABLE public.Staging_aggregations
(
	id varchar(256),
	doc_count int,
	fol_avg float,
	eng_avg float,
);

CREATE TABLE public.history
(
	id varchar(256),
	followers int,
	impressions int,
	reach int,
	doc_count int,
	fol_avg float,
	eng_avg float,
	colors varchar(1024),
);

CREATE TABLE public.color
(
	igId varchar(256),
	colors varchar(512),
);