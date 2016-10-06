CREATE KEYSPACE IF NOT EXISTS test_drive WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' }  AND DURABLE_WRITES = false;

CREATE TABLE IF NOT EXISTS test_drive.basic_data_type (
 	id_uuid uuid PRIMARY KEY,
 	str_ascii ascii,
	num_big_integer bigint,
	binary_data blob,
	true_or_false boolean,
	date_date date,
	num_decimal decimal,
	num_double double,
	num_float float,
	net_inet inet,
	num_int int,
	num_small_int smallint,
	str_text text,
	date_time time,
	date_timestamp timestamp,
	id_timeuuid timeuuid,
	num_tiny_int tinyint,
	str_varchar varchar,
	num_varint varint
);
-- the secondary index below is required when testing against Cassandra 2.2
CREATE INDEX IF NOT EXISTS str_text_idx ON test_drive.basic_data_type (str_text);
TRUNCATE TABLE test_drive.basic_data_type;

CREATE TABLE IF NOT EXISTS test_drive.static_data_type (
	id1 uuid,
	id2 uuid,
 	id_uuid uuid static,
 	str_ascii ascii static,
	num_big_integer bigint static,
	binary_data blob static,
	true_or_false boolean static,
	date_date date static,
	num_decimal decimal static,
	num_double double static,
	num_float float static,
	net_inet inet static,
	num_int int static,
	num_small_int smallint static,
	str_text text static,
	date_time time static,
	date_timestamp timestamp static,
	id_timeuuid timeuuid static,
	num_tiny_int tinyint static,
	str_varchar varchar static,
	num_varint varint static,
	primary key (id1, id2)
);
TRUNCATE TABLE test_drive.static_data_type;

CREATE TABLE IF NOT EXISTS test_drive.counter_data_type (
 	id_uuid uuid PRIMARY KEY,
 	num_counter counter
);
TRUNCATE TABLE test_drive.counter_data_type;

CREATE TABLE IF NOT EXISTS test_drive.list_data_type (
 	id uuid PRIMARY KEY,
 	id_uuid list<uuid>,
 	str_ascii list<ascii>,
	num_big_integer list<bigint>,
	binary_data list<blob>,
	true_or_false list<boolean>,
	date_date list<date>,
	num_decimal list<decimal>,
	num_double list<double>,
	num_float list<float>,
	net_inet list<inet>,
	num_int list<int>,
	num_small_int list<smallint>,
	str_text list<text>,
	date_time list<time>,
	date_timestamp list<timestamp>,
	id_timeuuid list<timeuuid>,
	num_tiny_int list<tinyint>,
	str_varchar list<varchar>,
	num_varint list<varint>
);
TRUNCATE TABLE test_drive.list_data_type;

CREATE TABLE IF NOT EXISTS test_drive.set_data_type (
 	id uuid PRIMARY KEY,
 	id_uuid set<uuid>,
 	str_ascii set<ascii>,
	num_big_integer set<bigint>,
	binary_data set<blob>,
	true_or_false set<boolean>,
	date_date set<date>,
	num_decimal set<decimal>,
	num_double set<double>,
	num_float set<float>,
	net_inet set<inet>,
	num_int set<int>,
	num_small_int set<smallint>,
	str_text set<text>,
	date_time set<time>,
	date_timestamp set<timestamp>,
	id_timeuuid set<timeuuid>,
	num_tiny_int set<tinyint>,
	str_varchar set<varchar>,
	num_varint set<varint>
);
TRUNCATE TABLE test_drive.set_data_type;

CREATE TABLE IF NOT EXISTS test_drive.tuple_data_type (
    id uuid PRIMARY KEY,
    simple_tuple tuple<ascii, bigint, blob, boolean, date, decimal, double, float, inet, int, smallint, text, time, timestamp, timeuuid, tinyint, uuid, varchar, varint>,
    nested_tuple tuple<tuple<text>>
);
TRUNCATE TABLE test_drive.tuple_data_type;

CREATE TABLE IF NOT EXISTS test_drive.map_data_type (
	id uuid PRIMARY KEY,
	id_uuid map<uuid, uuid>,
	str_ascii map<ascii, ascii>,
	num_big_integer map<bigint, bigint>,
	binary_data map<blob, blob>,
	true_or_false map<boolean, boolean>,
	date_date map<date, date>,
	num_decimal map<decimal, decimal>,
	num_double map<double, double>,
	num_float map<float, float>,
	net_inet map<inet, inet>,
	num_int map<int, int>,
	num_small_int map<smallint, smallint>,
	str_text map<text, text>,
	date_time map<time, time>,
	date_timestamp map<timestamp, timestamp>,
	id_timeuuid map<timeuuid, timeuuid>,
	num_tiny_int map<tinyint, tinyint>,
	str_varchar map<varchar, varchar>,
	num_varint map<varint, varint>
);
TRUNCATE TABLE test_drive.map_data_type;

CREATE TABLE IF NOT EXISTS test_drive.frozen_data_type (
 	id_uuid uuid PRIMARY KEY,
 	fz_list frozen<list<text>>,
 	nested_fz_list list<frozen<list<text>>>,
 	fz_set frozen<set<text>>,
    nested_fz_set set<frozen<set<text>>>,
    fz_map frozen<map<text,text>>,
    nested_fz_map map<text,frozen<map<text,text>>>
);
TRUNCATE TABLE test_drive.frozen_data_type;

CREATE TYPE IF NOT EXISTS test_drive.simple_user_type (
 	str_ascii ascii,
	num_big_integer bigint,
	binary_data blob,
	true_or_false boolean,
	date_date date,
	num_decimal decimal,
	num_double double,
	num_float float,
	net_inet inet,
	num_int int,
	num_small_int smallint,
	str_text text,
	date_time time,
	date_timestamp timestamp,
	id_timeuuid timeuuid,
	num_tiny_int tinyint,
	id_uuid uuid,
	str_varchar varchar,
	num_varint varint
);

CREATE TYPE IF NOT EXISTS test_drive.complex_user_type (
 	c_list list<text>,
 	c_set set<text>,
 	c_map map<text,text>,
 	c_tuple tuple<text>,
 	fz_list frozen<list<text>>,
 	fz_set frozen<set<text>>,
    fz_map frozen<map<text,text>>
);

CREATE TABLE IF NOT EXISTS test_drive.user_data_type (
 	id_uuid uuid PRIMARY KEY,
 	simple frozen<simple_user_type>,
 	complex frozen<complex_user_type>,
 	simple_list list<frozen<simple_user_type>>,
 	simple_set set<frozen<simple_user_type>>,
 	simple_map map<text,frozen<simple_user_type>>,
 	complex_list list<frozen<simple_user_type>>,
    complex_set set<frozen<simple_user_type>>,
    complex_map map<text,frozen<simple_user_type>>,
 	c_tuple tuple<frozen<simple_user_type>,frozen<complex_user_type>>
);
TRUNCATE TABLE test_drive.user_data_type;

-- test OLAP(use foodmart instead later)
CREATE TABLE IF NOT EXISTS test_drive.dim_company (
    company_id int PRIMARY KEY,
    company_type text,
    company_name text
);
TRUNCATE TABLE test_drive.dim_company;
INSERT INTO test_drive.dim_company(company_id, company_type, company_name) VALUES (1, 'T1', 'A');
INSERT INTO test_drive.dim_company(company_id, company_type, company_name) VALUES (2, 'T1', 'B');
INSERT INTO test_drive.dim_company(company_id, company_type, company_name) VALUES (3, 'T2', 'C');
INSERT INTO test_drive.dim_company(company_id, company_type, company_name) VALUES (4, 'T3', 'D');

CREATE TABLE IF NOT EXISTS test_drive.dim_date (
    date_id int PRIMARY KEY,
    year int,
    quarter int,
    month int,
    week int,
    day_of_week int,
    day_of_month int,
    date_value text
);
TRUNCATE TABLE test_drive.dim_date;
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16467,2015,1,2,5,1,1,'2015-02-01');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16468,2015,1,2,5,2,2,'2015-02-02');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16469,2015,1,2,5,3,3,'2015-02-03');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16470,2015,1,2,5,4,4,'2015-02-04');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16471,2015,1,2,5,5,5,'2015-02-05');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16472,2015,1,2,5,6,6,'2015-02-06');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16473,2015,1,2,5,7,7,'2015-02-07');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16474,2015,1,2,6,1,8,'2015-02-08');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16475,2015,1,2,6,2,9,'2015-02-09');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16476,2015,1,2,6,3,10,'2015-02-10');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16477,2015,1,2,6,4,11,'2015-02-11');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16478,2015,1,2,6,5,12,'2015-02-12');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16479,2015,1,2,6,6,13,'2015-02-13');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16480,2015,1,2,6,7,14,'2015-02-14');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16481,2015,1,2,7,1,15,'2015-02-15');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16482,2015,1,2,7,2,16,'2015-02-16');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16483,2015,1,2,7,3,17,'2015-02-17');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16484,2015,1,2,7,4,18,'2015-02-18');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16485,2015,1,2,7,5,19,'2015-02-19');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16486,2015,1,2,7,6,20,'2015-02-20');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16487,2015,1,2,7,7,21,'2015-02-21');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16488,2015,1,2,8,1,22,'2015-02-22');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16489,2015,1,2,8,2,23,'2015-02-23');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16490,2015,1,2,8,3,24,'2015-02-24');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16491,2015,1,2,8,4,25,'2015-02-25');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16492,2015,1,2,8,5,26,'2015-02-26');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16493,2015,1,2,8,6,27,'2015-02-27');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16494,2015,1,2,8,7,28,'2015-02-28');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16495,2015,1,3,9,1,1,'2015-03-01');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16496,2015,1,3,9,2,2,'2015-03-02');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16497,2015,1,3,9,3,3,'2015-03-03');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16498,2015,1,3,9,4,4,'2015-03-04');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16499,2015,1,3,9,5,5,'2015-03-05');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16500,2015,1,3,9,6,6,'2015-03-06');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16501,2015,1,3,9,7,7,'2015-03-07');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16502,2015,1,3,10,1,8,'2015-03-08');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16503,2015,1,3,10,2,9,'2015-03-09');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16504,2015,1,3,10,3,10,'2015-03-10');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16505,2015,1,3,10,4,11,'2015-03-11');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16506,2015,1,3,10,5,12,'2015-03-12');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16507,2015,1,3,10,6,13,'2015-03-13');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16508,2015,1,3,10,7,14,'2015-03-14');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16509,2015,1,3,11,1,15,'2015-03-15');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16510,2015,1,3,11,2,16,'2015-03-16');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16511,2015,1,3,11,3,17,'2015-03-17');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16512,2015,1,3,11,4,18,'2015-03-18');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16513,2015,1,3,11,5,19,'2015-03-19');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16514,2015,1,3,11,6,20,'2015-03-20');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16515,2015,1,3,11,7,21,'2015-03-21');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16516,2015,1,3,12,1,22,'2015-03-22');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16517,2015,1,3,12,2,23,'2015-03-23');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16518,2015,1,3,12,3,24,'2015-03-24');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16519,2015,1,3,12,4,25,'2015-03-25');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16520,2015,1,3,12,5,26,'2015-03-26');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16521,2015,1,3,12,6,27,'2015-03-27');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16522,2015,1,3,12,7,28,'2015-03-28');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16523,2015,1,3,13,1,29,'2015-03-29');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16524,2015,1,3,13,2,30,'2015-03-30');
INSERT INTO test_drive.dim_date(date_id, year, quarter, month, week, day_of_week, day_of_month, date_value) VALUES (16525,2015,1,3,13,3,31,'2015-03-31');

CREATE TABLE IF NOT EXISTS test_drive.fact_transactions (
    tx_id uuid PRIMARY KEY,
    start_date_id int,
    end_date_id int,
    from_company int,
    to_company int,
    amount float
);
TRUNCATE TABLE test_drive.fact_transactions;
INSERT INTO test_drive.fact_transactions(tx_id, start_date_id, end_date_id, from_company, to_company, amount) VALUES (uuid(), 16472, 16502, 1, 2, 3.1);
INSERT INTO test_drive.fact_transactions(tx_id, start_date_id, end_date_id, from_company, to_company, amount) VALUES (uuid(), 16475, 16501, 3, 1, 2.2);
INSERT INTO test_drive.fact_transactions(tx_id, start_date_id, end_date_id, from_company, to_company, amount) VALUES (uuid(), 16505, 16520, 2, 1, 4.4);