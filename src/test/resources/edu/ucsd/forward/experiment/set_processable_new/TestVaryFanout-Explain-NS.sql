explain (analyze true, buffers false)
SELECT ALL
   __temp_8.p_brand AS p_brand, 
   __temp_8.total AS total, 
   __temp_8.__v10 AS __v10
FROM
   (
      SELECT ALL
         __temp_7.p_brand AS p_brand, 
         __temp_7.total AS total, 
         __temp_7.__v10 AS __v10,row_number() OVER(PARTITION BY __v10 ORDER BY total ASC NULLS LAST) AS __v12
      FROM
         (
            SELECT ALL
               __temp_5.__v5 AS p_brand, 
               COUNT(*) AS total, 
               __temp_4.__v9 AS __v10
            FROM
                  (
                     SELECT DISTINCT
                        __temp_3.__v9 AS __v9
                     FROM
                        (
                           SELECT ALL
                              __alias_1.s_suppkey AS __v9
                           FROM
                              public.supplier AS __alias_1
                        ) AS __temp_3
                  ) AS __temp_4
                  INNER JOIN 
                     (
                        SELECT ALL
                           p.p_brand AS __v5, 
                           p.p_partkey AS __v4
                        FROM
                           public.part AS p
                     ) AS __temp_5
                     INNER JOIN 
                     (
                        SELECT ALL
                           ps.ps_partkey AS __v3, 
                           ps.ps_suppkey AS __v2
                        FROM
                           public.partsupp AS ps
                     ) AS __temp_6
                     ON                   (__temp_6.__v3 = CAST(__temp_5.__v4 AS bigint))

                  ON                (CAST(__temp_4.__v9 AS bigint) = __temp_6.__v2)

            GROUP BY __temp_5.__v5, __temp_4.__v9
         ) AS __temp_7
   ) AS __temp_8
WHERE
   (__temp_8.__v12 <= 3)