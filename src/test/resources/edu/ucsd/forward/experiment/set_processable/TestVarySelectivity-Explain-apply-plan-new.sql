explain
SELECT ALL
   __temp_3.__v6 AS o_orderdate, 
   SUM(ALL __temp_3.__v7) AS sum_price
FROM
      (
         SELECT ALL
            c.c_nationkey AS __v5, 
            c.c_custkey AS __v4
         FROM
            public.customers_000 AS c
      ) AS __temp_2
      INNER JOIN 
      (
         SELECT ALL
            o.o_totalprice AS __v7, 
            o.o_orderdate AS __v6, 
            o.o_custkey AS __v3
         FROM
            public.orders AS o
      ) AS __temp_3
      ON    (__temp_3.__v3 = __temp_2.__v4)

WHERE
   (__temp_2.__v5 = 1)
GROUP BY __temp_3.__v6
ORDER BY SUM(ALL __temp_3.__v7) DESC NULLS LAST
FETCH NEXT 3 ROWS ONLY