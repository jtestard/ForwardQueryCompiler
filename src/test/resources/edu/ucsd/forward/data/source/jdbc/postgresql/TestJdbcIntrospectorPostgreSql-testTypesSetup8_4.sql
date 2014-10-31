CREATE SCHEMA introspect;

SET search_path to introspect;

CREATE TABLE introspect.supported_types (
	id 					serial primary key,
	bigint_				bigint,
	int8_				int8,
	bigserial_			bigserial,
	serial8_			serial8,
	bit_				bit,
	bit_varying			bit varying,
	varbit_				varbit,
	boolean_			boolean,
	bool_				bool,
	character_varying 	character varying,
	varchar_			varchar,
	character_			character,
	char_				char,
	date_				date,
	double_precision	double precision,
	float8_				float8,
	inet_				inet,
	integer_			integer,
	int_				int,
	int4_				int4,
	money_				money,
	numeric_			numeric,
	decimal_			decimal,
	real_				real,
	smallint_			smallint,
	serial_				serial,
	serial4_			serial4,
	text_				text,
	timestamp_			timestamp,
	timestamp_wo_time_zone	timestamp without time zone,
	timestamp_w_time_zone	timestamp with time zone,
	timestamptz_		timestamptz
);

-- Not Supported, schema should be empty
CREATE TABLE introspect.unsupported_types (
	id	integer, -- Necessary because Table.create requires table tuple to have more than one attribte
	box_				box,
	bytea_				bytea,
	cidr_				cidr,
	circle_				circle,
	line_				line,
	lseg_				lseg,
	macaddr_			macaddr,
	path_				path,
	point_				point,
	polygon_			polygon,
	time_				time,
	time_wo_time_zone	time without time zone,
	time_w_time_zone	time with time zone,
	timetz_				timetz,
	tsquery_			tsquery,
	tsvector_			tsvector,
	txid_snapshot_		txid_snapshot,
	uuid_				uuid,
	xml_				xml
);
