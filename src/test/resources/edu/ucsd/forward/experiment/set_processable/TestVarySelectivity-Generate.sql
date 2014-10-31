
-- Create tables from scratch;
drop table if exists nations_x000;
drop table if exists customers_x000;

-- Duplicate the 25 nations into 000 nations
create table nations_x000 as
select  i as n_nationkey, n_name, n_regionkey, n_comment
from    (select * from generate_series(0, 000 - 1)) as a(i)
        join nation on (i % 25 = n_nationkey);
        
-- Distribute existing customers among 000 nations
create table customers_x000 (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment) as
select  c_custkey, c_name, c_address,
        c_custkey % 000 as c_nationkey,
        c_phone, c_acctbal, c_mktsegment, c_comment
from    customers;

-- Create indexes
alter table nations_x000 add primary key (n_nationkey);
create index on customers_x000(c_nationkey);

-- Update statistics for optimizer
vacuum analyze customers_x000;
vacuum analyze nations_x000;