explain (analyze true, buffers true)
SELECT ALL
   __temp_2.__v5 AS p_brand, 
   COUNT(*) AS total
FROM
      (
         SELECT ALL
            ps.ps_partkey AS __v3, 
            ps.ps_suppkey AS __v2
         FROM
            public.partsupp AS ps
      ) AS __temp_1
      INNER JOIN 
      (
         SELECT ALL
            p.p_brand AS __v5, 
            p.p_partkey AS __v4
         FROM
            public.part AS p
      ) AS __temp_2
      ON    (__temp_1.__v3 = CAST(__temp_2.__v4 AS bigint))

WHERE
   (CAST(1 AS bigint) = __temp_1.__v2)
GROUP BY __temp_2.__v5
ORDER BY COUNT(*) DESC NULLS LAST
FETCH NEXT 3 ROWS ONLY