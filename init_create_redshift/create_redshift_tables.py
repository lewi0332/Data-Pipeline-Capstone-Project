import psycopg2
import configparser

config = configparser.ConfigParser()
config.read_file(open('../redshift.cfg'))

DWH_DB = config.get("DWH", "DWH_DB")
DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")
DWH_PORT = config.get("DWH", "DWH_PORT")

conn = psycopg2.connect(host='socialsystem.c6ilokaakqwl.us-east-1.redshift.amazonaws.com',
                        dbname=DWH_DB, user=DWH_DB_USER, password=DWH_DB_PASSWORD, port=DWH_PORT)

cur = conn.cursor()

cur.execute("""CREATE TABLE IF NOT EXISTS public.staging_users
(
	created_at varchar(256),
	id varchar(256),
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
);""")
conn.commit()


cur.execute("""
CREATE TABLE IF NOT EXISTS public.Staging_aggregations
(
	id varchar(256),
	doc_count INT,
	fol_avg REAL,
	eng_avg REAL);""")
conn.commit()

cur.execute("""	
CREATE TABLE IF NOT EXISTS public.history
(	
	id varchar(256) NOT NULL,
	followers VARCHAR(256),
	impressions VARCHAR(256),
	reach VARCHAR(256),
	doc_count int,
	fol_avg REAL,
	eng_avg REAL,
	colors varchar(1024),
	CONSTRAINT users_pkey PRIMARY KEY (id)
);""")
conn.commit()

cur.execute("""	
CREATE TABLE IF NOT EXISTS public.staging_color
(
	igId varchar(256),
	colors varchar(1024)
);""")
conn.commit()
