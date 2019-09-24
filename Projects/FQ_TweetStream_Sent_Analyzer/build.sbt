name := "FQ_TweetStream_Sent_Analyzer"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.0",
  "org.apache.spark" %% "spark-sql" % "2.2.0",
  "org.apache.spark" %% "spark-streaming" % "2.2.0",
  "org.apache.bahir" %% "spark-streaming-twitter" % "2.2.0"
)


libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.9.1"
libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.9.1" classifier "models"
libraryDependencies += "edu.stanford.nlp" % "stanford-parser" % "3.9.1"