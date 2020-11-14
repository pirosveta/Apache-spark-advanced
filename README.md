start-dfs.sh
start-yarn.sh
mvn package
spark-submit —class ru.bmstu.hadoop.AirportFlightStatistics —master yarn-client —num-executors 3 target/spark-examples-1.0-SNAPSHOT.jar
hadoop fs -copyToLocal output
