create keyspace if not exists datastax_loyalty WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1' };

use datastax_loyalty;

create table customer_points (
	id text,
	time timestamp,
	balance int static,
	balanceat timestamp static,
	value int,
	comment text,
	PRIMARY KEY(id,time)
)with clustering order by (time desc);