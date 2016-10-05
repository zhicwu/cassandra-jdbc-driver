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