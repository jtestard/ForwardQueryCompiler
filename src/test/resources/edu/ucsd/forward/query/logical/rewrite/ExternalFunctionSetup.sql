--drop schema if exists external cascade; 

create schema external;

set search_path to external;

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