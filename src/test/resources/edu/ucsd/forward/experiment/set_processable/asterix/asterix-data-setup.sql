drop dataverse TPCH if exists;
create dataverse  TPCH;
use dataverse TPCH;

create type nationType as open {
  n_nationkey: int32,
  n_name: string,
  n_regionkey: int32,
  n_comment: string
}

create dataset nation(nationType)
primary key n_nationkey;

create type customersType as open {
  c_custkey: int32,
  c_name: string,
  c_address: string,
  c_nationkey: int32,
  c_phone: string,
  c_acctbal: double,
  c_mktsegment: string,
  c_comment: string
}

create dataset customers(customersType)
primary key c_custkey;

create type ordersType as open {
  o_orderkey: int32,
  o_custkey: int32,
  o_orderstatus: string,
  o_totalprice: double,
  o_orderdate: string,
  o_orderpriority: string,
  o_clerk: string,
  o_shippriority: int32,
  o_comment: string
}

create dataset orders(ordersType)
primary key o_orderkey;

load dataset nation using localfs
(("path"="localhost:///Users/yupengf/asterix-mgmt/data/nation.adm"),("format"="adm"));

load dataset customers using localfs
(("path"="localhost:///Users/yupengf/asterix-mgmt/data/customers.adm"),("format"="adm"));

load dataset orders using localfs
(("path"="localhost:///Users/yupengf/asterix-mgmt/data/orders.adm"),("format"="adm"));

create type selectedNationType as open {
  sn_nationkey: int32
}

create dataset selectedNations(selectedNationType)
primary key sn_nationkey;

insert into dataset selectedNations (
 {"sn_nationkey":1}
)
insert into dataset selectedNations (
 {"sn_nationkey":2}
)
insert into dataset selectedNations (
 {"sn_nationkey":3}
)insert into dataset selectedNations (
 {"sn_nationkey":4}
)insert into dataset selectedNations (
 {"sn_nationkey":5}
)
insert into dataset selectedNations (
 {"sn_nationkey":6}
)
insert into dataset selectedNations (
 {"sn_nationkey":7}
)insert into dataset selectedNations (
 {"sn_nationkey":8}
)insert into dataset selectedNations (
 {"sn_nationkey":9}
)insert into dataset selectedNations (
 {"sn_nationkey":10}
)


 for $ds in dataset Metadata.Dataset return $ds;