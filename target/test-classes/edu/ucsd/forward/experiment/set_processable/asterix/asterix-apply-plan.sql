use dataverse TPCH;

for $nation in dataset nation
for $sn in dataset selectedNations
where $nation.n_nationkey = $sn.sn_nationkey  /*+ indexnl */
return {
  "nation_key": $nation.n_nationkey,
  "name": $nation.n_name,
  "aggregates": for $order in dataset orders
                for $customer in dataset customers
                where $order.o_custkey = $customer.c_custkey
                and  $customer.c_nationkey = $nation.n_nationkey
                group by $orderdate := $order.o_orderdate with $order
                let $sum := sum(for $o in $order return $o.o_totalprice)
                order by $sum desc
                limit 3
                return {
                  "order_date": $orderdate,
                  "sum_price": $sum
                }
}
