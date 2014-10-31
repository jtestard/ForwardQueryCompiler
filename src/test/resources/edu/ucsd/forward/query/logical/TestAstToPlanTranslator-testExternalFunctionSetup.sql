--drop schema if exists external cascade; 

--create schema external;

--set search_path to external;

-- Table: customer
CREATE TABLE public.customer
(
  cid integer NOT NULL,
  "name" character varying(30),
  state character(2),
  CONSTRAINT customer_pk PRIMARY KEY (cid)
)
WITH (
  OIDS=FALSE
);

INSERT INTO public.customer VALUES(1, 'Mike', 'CA');
INSERT INTO public.customer VALUES(6, 'Tom', 'CA');
INSERT INTO public.customer VALUES(21, 'Nick', 'NY');
INSERT INTO public.customer VALUES(24, 'Mary', 'MN');
INSERT INTO public.customer VALUES(30, 'Sam', null);
INSERT INTO public.customer VALUES(31, 'Sam', null);

-- Function: count_customers()
CREATE OR REPLACE FUNCTION public.count_customers()
  RETURNS bigint AS
$BODY$
    SELECT COUNT(cid) FROM public.customer AS result;
$BODY$
  LANGUAGE sql VOLATILE
  COST 100;

-- Function: multiply_scalar(integer)
CREATE OR REPLACE FUNCTION public.multiply_scalar(IN x integer, OUT twice integer)
  RETURNS integer AS
'SELECT $1 * 2'
  LANGUAGE sql VOLATILE
  COST 100;

-- Function: multiply_inout_scalar(integer)
CREATE OR REPLACE FUNCTION public.multiply_inout_scalar(INOUT x integer)
  RETURNS integer AS
'SELECT $1 * 2'
  LANGUAGE sql VOLATILE
  COST 100;

-- Function: multiply_tuple(integer)
CREATE OR REPLACE FUNCTION public.multiply_tuple(IN x integer, OUT twice integer, OUT thrice integer)
  RETURNS record AS
'SELECT $1 * 2, $1 * 3'
  LANGUAGE sql VOLATILE
  COST 100;

-- Function: multiply_inout_tuple(integer, integer)
CREATE OR REPLACE FUNCTION public.multiply_inout_tuple(INOUT x integer, INOUT y integer)
  RETURNS record AS
'SELECT $1 * 2, $2 * 3'
  LANGUAGE sql VOLATILE
  COST 100;

-- Function: get_customers(integer)
CREATE OR REPLACE FUNCTION public.get_customers(integer)
  RETURNS SETOF customer AS
$BODY$
    SELECT * FROM public.customer WHERE cid > $1;
$BODY$
  LANGUAGE sql VOLATILE
  COST 100
  ROWS 1000;

-- Function: get_customers_record(integer)
CREATE OR REPLACE FUNCTION public.get_customers_record(IN x integer, OUT cid integer, OUT state character)
  RETURNS SETOF record AS
$BODY$
    SELECT cid, state FROM public.customer WHERE cid > $1;
$BODY$
  LANGUAGE sql VOLATILE
  COST 100
  ROWS 1000;

-- Function: get_customers_table(integer)
CREATE OR REPLACE FUNCTION public.get_customers_table(IN x integer)
  RETURNS TABLE(cid integer, state character) AS
$BODY$
    SELECT cid, state FROM public.customer WHERE cid > $1;
$BODY$
  LANGUAGE sql VOLATILE
  COST 100
  ROWS 1000;

-- Function: no_ret(integer)
CREATE OR REPLACE FUNCTION public.void(x integer)
  RETURNS void AS
'UPDATE customer SET state = ''CA'' WHERE cid = $1'
  LANGUAGE sql VOLATILE
  COST 100;

-- Function: identity(customer)
CREATE OR REPLACE FUNCTION public.invalid(customer)
  RETURNS SETOF customer AS
$BODY$
    SELECT $1;
$BODY$
  LANGUAGE sql VOLATILE
  COST 100
  ROWS 1000;
