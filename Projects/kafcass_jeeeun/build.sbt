name := "kafcass_jeeeun"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.4"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.4"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.4"
libraryDependencies += "org.apache.spark" %% "spark-streaming-twitter" % "1.6.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % "1.6.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.3.1"
libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.5.2" artifacts (Artifact("stanford-corenlp", "models"), Artifact("stanford-corenlp"))
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.3.0"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.1"