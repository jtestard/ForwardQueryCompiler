create schema external;

set search_path to external;

-- Table: customer
CREATE TABLE external.customer
(
  cid integer NOT NULL,
  "name" character varying(30),
  state character(2),
  CONSTRAINT customer_pk PRIMARY KEY (cid)
)
WITH (
  OIDS=FALSE
);

INSERT INTO external.customer VALUES(1, 'Mike', 'CA');
INSERT INTO external.customer VALUES(6, 'Tom', 'CA');
INSERT INTO external.customer VALUES(21, 'Nick', 'NY');
INSERT INTO external.customer VALUES(24, 'Mary', 'MN');
INSERT INTO external.customer VALUES(30, 'Sam', null);
INSERT INTO external.customer VALUES(31, 'Sam', null);

-- Function: count_customers()
CREATE OR REPLACE FUNCTION external.count_customers()
  RETURNS bigint AS
$BODY$
    SELECT COUNT(cid) FROM external.customer AS result;
$BODY$
  LANGUAGE sql VOLATILE
  COST 100;

-- Function: multiply_scalar(integer)
CREATE OR REPLACE FUNCTION external.multiply_scalar(IN x integer, OUT twice integer)
  RETURNS integer AS
'SELECT $1 * 2'
  LANGUAGE sql VOLATILE
  COST 100;

-- Function: multiply_inout_scalar(integer)
CREATE OR REPLACE FUNCTION external.multiply_inout_scalar(INOUT x integer)
  RETURNS integer AS
'SELECT $1 * 2'
  LANGUAGE sql VOLATILE
  COST 100;

-- Function: multiply_tuple(integer)
CREATE OR REPLACE FUNCTION external.multiply_tuple(IN x integer, OUT twice integer, OUT thrice integer)
  RETURNS record AS
'SELECT $1 * 2, $1 * 3'
  LANGUAGE sql VOLATILE
  COST 100;

-- Function: multiply_inout_tuple(integer, integer)
CREATE OR REPLACE FUNCTION external.multiply_inout_tuple(INOUT x integer, INOUT y integer)
  RETURNS record AS
'SELECT $1 * 2, $2 * 3'
  LANGUAGE sql VOLATILE
  COST 100;

-- Function: get_customers(integer)
CREATE OR REPLACE FUNCTION external.get_customers(integer)
  RETURNS SETOF customer AS
$BODY$
    SELECT * FROM external.customer WHERE cid > $1;
$BODY$
  LANGUAGE sql VOLATILE
  COST 100
  ROWS 1000;

-- Function: get_customers_record(integer)
CREATE OR REPLACE FUNCTION external.get_customers_record(IN x integer, OUT cid integer, OUT state character)
  RETURNS SETOF record AS
$BODY$
    SELECT cid, state FROM external.customer WHERE cid > $1;
$BODY$
  LANGUAGE sql VOLATILE
  COST 100
  ROWS 1000;

-- Function: get_customers_table(integer)
CREATE OR REPLACE FUNCTION external.get_customers_table(IN x integer)
  RETURNS TABLE(cid integer, state character) AS
$BODY$
    SELECT cid, state FROM external.customer WHERE cid > $1;
$BODY$
  LANGUAGE sql VOLATILE
  COST 100
  ROWS 1000;

-- Function: no_ret(integer)
CREATE OR REPLACE FUNCTION external.void(x integer)
  RETURNS void AS
'UPDATE customer SET state = ''CA'' WHERE cid = $1'
  LANGUAGE sql VOLATILE
  COST 100;

-- Function: identity(customer)
CREATE OR REPLACE FUNCTION external.invalid(customer)
  RETURNS SETOF customer AS
$BODY$
    SELECT $1;
$BODY$
  LANGUAGE sql VOLATILE
  COST 100
  ROWS 1000;
