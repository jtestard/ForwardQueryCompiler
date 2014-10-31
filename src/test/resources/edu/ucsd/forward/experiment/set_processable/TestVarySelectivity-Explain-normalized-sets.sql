create temporary table __param_3 (
	n_nationkey int4 default null
);
insert into __param_3 (n_nationkey) values (1);
insert into __param_3 (n_nationkey) values (2);
insert into __param_3 (n_nationkey) values (3);
insert into __param_3 (n_nationkey) values (4);
insert into __param_3 (n_nationkey) values (5);
insert into __param_3 (n_nationkey) values (6);
insert into __param_3 (n_nationkey) values (7);
insert into __param_3 (n_nationkey) values (8);
insert into __param_3 (n_nationkey) values (9);
insert into __param_3 (n_nationkey) values (10);
analyze __param_3;
----
explain
SELECT ALL
   __temp_6.nationkey AS nationkey, 
   __temp_6.o_orderdate AS o_orderdate, 
   __temp_6.sum_call__v28 AS sum_call__v28
FROM
   (
      SELECT ALL
          row_number() over(PARTITION BY nationkey order by sum_call__v28) AS __v0, 
         __temp_5.nationkey AS nationkey, 
         __temp_5.o_orderdate AS o_orderdate, 
         __temp_5.sum_call__v28 AS sum_call__v28
      FROM
         (
            SELECT ALL
               __temp_2.nationkey AS nationkey, 
               __temp_3.o_orderdate AS o_orderdate, 
               SUM(ALL __temp_3.o_totalprice) AS sum_call__v28
            FROM
                  (
                     SELECT ALL
                        __param_3.n_nationkey AS nationkey
                     FROM
                        __param_3 AS __param_3
                  ) AS __temp_2
                  INNER JOIN 
                     (
                        SELECT ALL
                           o.o_custkey AS o_custkey, 
                           o.o_totalprice AS o_totalprice, 
                           o.o_orderdate AS o_orderdate
                        FROM
                           public.orders AS o
                     ) AS __temp_3
                     INNER JOIN 
                     (
                        SELECT ALL
                           c.c_custkey AS c_custkey, 
                           c.c_nationkey AS c_nationkey
                        FROM
                           public.customers_000 AS c
                     ) AS __temp_4
                     ON                   (__temp_3.o_custkey = __temp_4.c_custkey)

                  ON                (__temp_2.nationkey = __temp_4.c_nationkey)

            GROUP BY __temp_2.nationkey, __temp_3.o_orderdate
         ) AS __temp_5
   ) AS __temp_6
WHERE
   (__temp_6.__v0 <= 3)
;
