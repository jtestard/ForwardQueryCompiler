create temporary table selected_nations (
	n_nationkey int4 default null
);
insert into selected_nations (n_nationkey) values (1);
insert into selected_nations (n_nationkey) values (2);
insert into selected_nations (n_nationkey) values (3);
insert into selected_nations (n_nationkey) values (4);
insert into selected_nations (n_nationkey) values (5);
insert into selected_nations (n_nationkey) values (6);
insert into selected_nations (n_nationkey) values (7);
insert into selected_nations (n_nationkey) values (8);
insert into selected_nations (n_nationkey) values (9);
insert into selected_nations (n_nationkey) values (10);
analyze selected_nations;
----
 
select n_nationkey, n_name,sum_price, o_orderdate,
          case when sum_price is null and o_orderdate is null then null else rn end as h
 from (select n_nationkey, n_name,sum_price, o_orderdate,
           case when sum_price is null and o_orderdate is null then null else rn end as rn
    from (select  n_nationkey, n_name,sum_price, o_orderdate,
               row_number() OVER(PARTITION BY n_nationkey, n_name ORDER BY sum_price DESC NULLS LAST) AS rn  
      from(  
        select n_nationkey, n_name,
               case when g is null then null else sum_price end as sum_price,
               case when g is null then null else o_orderdate end as o_orderdate
        from (
            select n_nationkey, n_name, g, o_orderdate, sum(o_totalprice) as sum_price    
            from (
                    select t1.n_nationkey, t1.n_name, t2.o_totalprice,o_orderdate,
                    case when o_orderdate is null and o_totalprice is null then null
                            else true end as g
                    from
                        (select n.n_nationkey, n.n_name
                        from selected_nations as sn join public.nation as n on sn.n_nationkey = n.n_nationkey) as t1
                        left outer join
                        (select o_totalprice, o_orderdate, c_nationkey
                         from public.customers as c join public.orders as o on c.c_custkey = o.o_custkey) as t2
                        on t1.n_nationkey = t2.c_nationkey
                    ) t3
            group by n_nationkey, n_name, g, o_orderdate) as t4
         ) as t5    
         ) as t7) as t8
 where rn <= 3 or (sum_price is null and o_orderdate is null)     