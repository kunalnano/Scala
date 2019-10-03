name := "cassandra_cluster"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= {

  Seq(
    "com.typesafe.akka"       %% "akka-actor"                             % "2.4.14",
    "com.typesafe.akka"       %% "akka-stream"                            % "2.4.14",
    "com.lightbend.akka"      %% "akka-stream-alpakka-cassandra"          % "0.15",
    "com.lightbend.akka"      %% "akka-stream-alpakka-elasticsearch"      % "1.0-M2",
    "com.typesafe.akka"       %% "akka-stream-kafka"                      % "0.21.1",
    "io.spray"                %% "spray-json"                             % "1.3.5",
    "com.typesafe.akka"       %% "akka-slf4j"                             % "2.4.14",
    "org.slf4j"               % "slf4j-api"                               % "1.7.5",
    "ch.qos.logback"          % "logback-classic"                         % "1.0.9",
    "org.scalatest"           % "scalatest_2.11"                          % "2.2.1"               % "test"
  )
}

resolvers ++= Seq(
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"
)