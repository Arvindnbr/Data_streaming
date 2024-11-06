# Data_streaming
Real time data streaming using apache kafka

This project aim to an end to end stream realtime data from https://randomuser.me/api/ and streams all the data of users to confluent kafka using airflow DAG.
confluent kafka relies on control centre, kafka broker (Which creates kafka topics), zookeeper and a webserver that manages the stream.
Next the spark master worker nodes distributed approach writes the data to the cassandra db in a distributed manner.


this is the archietectre

--------API----------           -----Data stream-----               ------distributed working------


randuser -> airflow ->          confluentkafka broker ->                apache spark + cassandra
               +                          +                    
          postgressql         zookeeper + control centre



the java jar configs were used from mvnrepository
https://mvnrepository.com/artifact/com.datastax.spark/spark-cassandra-connector

and for kafka sql connector
https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10


finally run the spark worker by

/// spark-submit --master spark://localhost:7050     --conf spark.driver.host=localhost     --conf spark.driver.bindAddress=0.0.0.0     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,com.datastax.spark:spark-cassandra-connector_2.12:3.3.0     spark.py ///

