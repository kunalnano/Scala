import java.util.Date

import com.datastax.driver.core.Cluster
import com.datastax.spark.connector._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


object casskafka {

  def main(args: Array[String]): Unit = {

    //We need batch processing configuration
    val conf = new SparkConf()
      .setAppName("Al")
      .setMaster("local[*]")

    val cassandra_host = conf.get("spark.cassandra.connection.host", "localhost");
    // val sc = new SparkContext("127.0.0.1:9042", "test", conf)

    val topics = List("Israel").toSet // You have two topics; "israel" and "manny"

    // connect directly to Cassandra from the driver to create the keyspace
    val cluster = Cluster.builder().addContactPoint(cassandra_host).build()
    val session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS Tweet WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    session.execute("CREATE TABLE IF NOT EXISTS Tweet.Twitter (word text, ts timestamp, count int, PRIMARY KEY(word, ts)) ")
    //session.execute("TRUNCATE Tweet")

    val kafkaParams = Map( //Parameters to connect to Kafka
      "bootstrap.servers" -> "localhost:9092", //This's Al's IP. if you want local; "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark-streaming-something",
      "auto.offset.reset" -> "earliest"
    )

    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")
    val line = KafkaUtils.createDirectStream[String,String](
      //Parameter for Streaming Context
      ssc,
      PreferConsistent,
      Subscribe[String,String](topics, kafkaParams)
    )

    // val tmp = line.map(record => record.value().toString)
    // val _line = tmp.flatMap(_.split(","))
    val NFL = line.map(_.value) // split the message into lines
      .flatMap(_.split(" ")) //split into words
      .filter(w => w.length() > 0) // remove any empty words caused by double spaces
      .map(w => (w, 1L)).reduceByKey(_ + _) // count by wordline.foreachRDD(rdd => {
      .map({case (w,c) => (w,new Date().getTime,c)}) // add the current timestamp saveToCassandra("Tweet,count")})
    NFL.print()

    //val c = CassandraConnector(sc.getConf)
    //c.withSessionDo(session => session.execute("INSERT INTO countries.countries(id,name) VALUES (3,'Test3')"))
    NFL.foreachRDD(rdd => {
      rdd.saveToCassandra("tweet","twitter")
    })

    ssc.start() // start the streaming context
    ssc.awaitTermination() // block while the context is running (until it's stopped by the timer)
    ssc.stop()

    // Get the results using spark SQL
    val sc = new SparkContext(conf) // create a new spark core context
    val rdd1 = sc.cassandraTable("tweet","twitter")
    rdd1.take(100).foreach(println)
    sc.stop()

  }

}
