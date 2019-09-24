name := "sparkStreaming"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.twitter4j" % "twitter4j-core" % "4.0.6"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka" % "1.6.0"
libraryDependencies += "org.apache.spark" % "spark-streaming-twitter" % "1.6.0"

