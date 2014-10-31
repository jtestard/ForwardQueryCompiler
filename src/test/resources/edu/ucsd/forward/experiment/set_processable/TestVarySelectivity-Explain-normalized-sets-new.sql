create temporary table __temp_0 (
	n_nationkey int4 default null
);
insert into __temp_0 (n_nationkey) values (1);
insert into __temp_0 (n_nationkey) values (2);
insert into __temp_0 (n_nationkey) values (3);
insert into __temp_0 (n_nationkey) values (4);
insert into __temp_0 (n_nationkey) values (5);
insert into __temp_0 (n_nationkey) values (6);
insert into __temp_0 (n_nationkey) values (7);
insert into __temp_0 (n_nationkey) values (8);
insert into __temp_0 (n_nationkey) values (9);
insert into __temp_0 (n_nationkey) values (10);
analyze __temp_0;
----
explain
SELECT ALL
   __temp_9.o_orderdate AS o_orderdate, 
   __temp_9.sum_price AS sum_price, 
   __temp_9.__v13 AS __v13
FROM
   (
      SELECT ALL
         __temp_8.o_orderdate AS o_orderdate, 
         __temp_8.sum_price AS sum_price, 
         __temp_8.__v13 AS __v13,row_number() OVER(PARTITION BY __v13 ORDER BY sum_price ASC NULLS LAST) AS __v16
      FROM
         (
            SELECT ALL
               __temp_6.__v6 AS o_orderdate, 
               SUM(ALL __temp_6.__v7) AS sum_price, 
               __temp_5.__v12 AS __v13
            FROM
                  (
                     SELECT DISTINCT
                        __temp_4.__v12 AS __v12
                     FROM
                        (
                           SELECT ALL
                              __alias_1.n_nationkey AS __v12
                           FROM
                              __temp_0 AS __alias_1
                        ) AS __temp_4
                  ) AS __temp_5
                  INNER JOIN 
                     (
                        SELECT ALL
                           o.o_totalprice AS __v7, 
                           o.o_orderdate AS __v6, 
                           o.o_custkey AS __v3
                        FROM
                           public.orders AS o
                     ) AS __temp_6
                     INNER JOIN 
                     (
                        SELECT ALL
                           c.c_nationkey AS __v5, 
                           c.c_custkey AS __v4
                        FROM
                           public.customers_000 AS c
                     ) AS __temp_7
                     ON                   (__temp_6.__v3 = __temp_7.__v4)

                  ON                (__temp_7.__v5 = __temp_5.__v12)

            GROUP BY __temp_6.__v6, __temp_5.__v12
         ) AS __temp_8
   ) AS __temp_9
WHERE
   (__temp_9.__v16 <= 3)
