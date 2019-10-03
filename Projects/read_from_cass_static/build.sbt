name := "read_from_cass_static"

version := "0.1"

scalaVersion := "2.11.8"
// https://mvnrepository.com/artifact/com.datastax.spark/spark-cassandra-connector
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.3.3"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.4"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.4"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.4"