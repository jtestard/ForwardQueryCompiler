DROP SCHEMA IF EXISTS introspect CASCADE;

CREATE SCHEMA introspect;

SET search_path to introspect;

CREATE DOMAIN postal_code AS TEXT
CHECK(
   	VALUE ~ E'^\\d{5}$'
	OR VALUE ~ E'^\\d{5}-\d{4}$'
);

CREATE TYPE dup_result AS (d1 int, d2 text);

CREATE TABLE t(
 id serial primary key
);

/************************************
 * Function with unsupported types 
 ************************************/
CREATE FUNCTION typs(point) RETURNS point AS $$
	SELECT $1;
$$ LANGUAGE SQL;

/************************************
 * Function returns single column  
 ************************************/
CREATE FUNCTION returnVoid() RETURNS void AS $$
	delete from t;
$$ LANGUAGE SQL;

/* Function returns typtype 'b' */
CREATE FUNCTION add(integer, integer) RETURNS integer AS $$
	SELECT $1 + $2
$$ LANGUAGE SQL;

CREATE FUNCTION dble(integer, out res integer) RETURNS integer AS $$
	SELECT $1*2
$$ LANGUAGE SQL;

/* Function returns typtype 'd' domain */
CREATE FUNCTION getZip(integer) RETURNS postal_code AS $$
	SELECT CAST($1 AS postal_code)
$$ LANGUAGE SQL;

/*-- Following functions return typtype 'p' --*/

/* Function returns using single out */
-- inferred return type
CREATE FUNCTION getValue1(out a1 int) AS $$
	SELECT 1
$$ LANGUAGE SQL;

-- explicit return type
CREATE FUNCTION getValue2(out a1 int) RETURNS integer AS $$
	SELECT 2
$$ LANGUAGE SQL;

/* Function returns using single inout */
-- inferred return type
CREATE FUNCTION getValue3(inout a1 int) AS $$ 
	SELECT $1
$$ LANGUAGE SQL;

-- explicit return type
CREATE FUNCTION getValue4(inout a1 int) RETURNS integer AS $$
	SELECT $1 + $1
$$ LANGUAGE SQL;

/************************************
 * Function returns multiple rows 
 ************************************/
/*--- Single column ---*/
CREATE FUNCTION getSingleColumnRows1() RETURNS SETOF integer AS $$
	SELECT 1 as id union SELECT 2 as id;
$$ LANGUAGE SQL;

CREATE FUNCTION getSingleColumnRows2(out id integer) RETURNS SETOF integer AS $$
	SELECT 3 as id union SELECT 4 as id;
$$ LANGUAGE SQL;

CREATE FUNCTION getSingleColumnRows3(inout id integer) RETURNS SETOF integer AS $$
	SELECT $1 as id union SELECT $1+1 as id;
$$ LANGUAGE SQL;

CREATE FUNCTION getSingleColumnRows4() RETURNS TABLE (id integer) AS $$
	SELECT 5 as id union SELECT 6 as id;
$$ LANGUAGE SQL;

CREATE FUNCTION getSingleColumnRows5(int, inout id integer) RETURNS SETOF integer AS $$
	SELECT $1 as id union SELECT $1+$2 as id;
$$ LANGUAGE SQL;


/************************************
 * Function returns multi-column 
 * (not imported because not supported)
 ************************************/
/* Using composite types */
CREATE FUNCTION dup1(int) RETURNS dup_result AS $$ 
    SELECT $1, CAST($1 AS text) || ' is text'
$$ LANGUAGE SQL;

/* Using multiple outs */
CREATE FUNCTION dup2(in int, out f1 int, out f2 text) AS $$ 
    SELECT $1, CAST($1 AS text) || ' is text' 
$$ LANGUAGE SQL;
    
/* Using multiple inouts */
CREATE FUNCTION dup3(inout g1 int, inout g2 text) AS $$
    SELECT $1, CAST($1 AS text) ||$2|| ' is text'
$$ LANGUAGE SQL;

/* Using mixing outs and inouts */
CREATE FUNCTION dup4(inout h1 int, out h2 text) AS $$
    SELECT $1, CAST($1 AS text) || ' is text'
$$ LANGUAGE SQL;

/* Using variadic */
CREATE FUNCTION stats(VARIADIC numeric[], out i1 numeric, out i2 numeric) AS $$
	SELECT min($1[i]), max($1[i]) FROM generate_subscripts($1, 1) g(i);
$$ LANGUAGE SQL;

/*--- Muti-column ---*/
CREATE FUNCTION getMultiColumnRows1() RETURNS SETOF dup_result AS $$
	SELECT 1 as id, 'id:1' as val union SELECT 2 as id, 'id:2' as val;
$$ LANGUAGE SQL;

CREATE FUNCTION getMultiColumnRows2(out id integer, out val text) RETURNS SETOF RECORD AS $$
	SELECT 3 as id, 'id:3' as val union SELECT 4 as id, 'id:4' as val;
$$ LANGUAGE SQL;

CREATE FUNCTION getMultiColumnRows3(inout id integer, inout val text) RETURNS SETOF RECORD AS $$
	SELECT $1 as id, 'val:'||$1||':'||$2 as val union SELECT $1+1 as id, 'val:'||$1+1||':'||$2 as val;
$$ LANGUAGE SQL;

CREATE FUNCTION getMultiColumnRows4() RETURNS TABLE (id integer, val text) AS $$
	SELECT 5 as id, 'id:5' as val union SELECT 6 as id, 'id:6' as val;
$$ LANGUAGE SQL;

CREATE FUNCTION getMultiColumnRows5(inout id integer, out val text) RETURNS SETOF RECORD AS $$
	SELECT $1 as id, 'val:'||$1 as val union SELECT $1+1 as id, 'val:'||$1+1 as val;
$$ LANGUAGE SQL;
