create keyspace if not exists loyalty_demo WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1' };

use loyalty_demo;

create table if not exists user_points (
	id text,
	time timestamp,
	balance int static,
	balanceat timestamp static,
	value int,
	comment text,
	PRIMARY KEY(id,time)
)with clustering order by (time desc);