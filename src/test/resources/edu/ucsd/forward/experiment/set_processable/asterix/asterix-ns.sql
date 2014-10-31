use dataverse TPCH;

let $t := for $nation in dataset nation
for $sn in dataset selectedNations
where $nation.n_nationkey = $sn.sn_nationkey  /*+ indexnl */
return {
	"nation_key": $nation.n_nationkey,
	"n_name": $nation.n_name
}

let $X := (
for $n in $t
for $customer in dataset customers
for $order in dataset orders
where $order.o_custkey = $customer.c_custkey
and  $customer.c_nationkey = $n.nation_key
group by $orderdate := $order.o_orderdate, $nation_key := $n.nation_key with $order
let $sum := sum(for $o in $order return $o.o_totalprice)
return {
	"nation_key": $nation_key,
    "order_date": $orderdate,
    "sum_price": $sum 
})

for $x in $X
group by $nation_key := $x.nation_key with $x
return {
    "nation_key": $nation_key,
    "aggregates": for $y in $x
                  order by $y.sum_price desc
                  limit 3
                  return {
                  	"order_date": $y.orderdate,
                    "sum_price": $y.sum
                  }
}

