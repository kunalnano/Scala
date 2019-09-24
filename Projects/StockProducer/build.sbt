name := "StockProducer"

version := "0.1"

scalaVersion := "2.11.8"


libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.1"

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % "1.6.3"

libraryDependencies += "org.apache.spark" %% "spark-streaming-twitter" % "1.6.3"

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.3.1"