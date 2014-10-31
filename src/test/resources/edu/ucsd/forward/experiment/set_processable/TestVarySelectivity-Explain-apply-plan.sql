explain
SELECT ALL
   __temp_2.o_orderdate AS o_orderdate, 
   SUM(ALL __temp_2.o_totalprice) AS sum_call__v28
FROM
      (
         SELECT ALL
            o.o_custkey AS o_custkey, 
            o.o_totalprice AS o_totalprice, 
            o.o_orderdate AS o_orderdate
         FROM
            public.orders AS o
      ) AS __temp_2
      INNER JOIN 
      (
         SELECT ALL
            c.c_custkey AS c_custkey, 
            c.c_nationkey AS c_nationkey
         FROM
            public.customers_000 AS c
      ) AS __temp_3
      ON    (__temp_2.o_custkey = __temp_3.c_custkey)

WHERE
   (1 = __temp_3.c_nationkey)
GROUP BY __temp_2.o_orderdate
ORDER BY SUM(ALL __temp_2.o_totalprice) ASC NULLS LAST
FETCH NEXT 3 ROWS ONLY
;