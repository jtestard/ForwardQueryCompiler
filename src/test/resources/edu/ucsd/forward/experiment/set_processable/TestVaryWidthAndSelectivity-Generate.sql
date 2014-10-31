CREATE OR REPLACE FUNCTION random_bytea(bytea_length integer)
RETURNS text AS $body$
    SELECT string_agg(lpad(to_hex(width_bucket(random(), 0, 1, 256)-1),2,'0') ,'')
    FROM generate_series(1, $1);
$body$
LANGUAGE 'sql'
VOLATILE
SET search_path = 'pg_catalog';

-- Create tables from scratch;
drop table if exists nations_x000_wXXX;

-- Duplicate the 25 nations into 000 nations
select i as n_nationkey, n_regionkey, n_comment,random_bytea(XXX/2) as n_name
into nations_x000_wXXX
from    (select * from generate_series(0, 000 - 1)) as a(i)
        join nation on (i % 25 = n_nationkey);
        
-- Create indexes
alter table nations_x000_wXXX add primary key (n_nationkey);

-- Update statistics for optimizer
vacuum analyze nations_x000_wXXX;

DROP FUNCTION random_bytea(bytea_length integer);