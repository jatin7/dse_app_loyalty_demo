Loyalty Points Demo
========================
To create the schema, run the following

	mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaSetup" -DcontactPoints=localhost
	
To create some loyalty points and redeems, run the following 
	
	mvn clean compile exec:java -Dexec.mainClass="com.datastax.loyalty.Main"  -DcontactPoints=localhost

To get the current (live) balance 
```
select sum(value) from customer_points where id = '0';
```


To remove the tables and the schema, run the following.

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaTeardown"
    
    
