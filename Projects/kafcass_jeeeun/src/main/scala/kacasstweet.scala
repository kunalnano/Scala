
import java.util.Date

import com.datastax.driver.core.Cluster
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object kacasstweet {

  def main(args: Array[String]): Unit = {

    //Start a new Spark Configuration
    val conf = new SparkConf()
      .setAppName("Al")
      .setMaster("local[*]")

    // Link SparkStreamingContext and SparkConf with new SparkContext to connect with Spark Cluster
    val ssc = new StreamingContext(conf, Seconds(5))
    val sc = ssc.sparkContext

    //Set LogLevel to "WARM" Anything that can potentially cause application oddities, with automatic recovery
    //Set LogLevel to "Error" Any error which is fatal to the operation, but not the service or application
    sc.setLogLevel("WARN")

    //Set Topic
    val topic = List("Israel").toSet

    //Define and call Kafka Parameters to connect to existing Kakfa instance
    //Since kafka is running local -> bootstrap.servers -> localhost:9092
    val kafkaParams = Map(
    "bootstrap.servers"->"localhost:9092",

    // Deserialize a String buffer into an object, given the class of the object.
    "key.deserializer"->classOf[StringDeserializer],
    "value.deserializer"->classOf[StringDeserializer],

    //Assign a group ID for kafka stream consumers
    "group.id"->"spark-stream-twitter",
    // earliest: automatically reset the offset to the earliest offset
    "auto.offset.reset"->"earliest"
    )

    // Connect Spark to Cassandra
    val cassandra_host = conf.get("spark.cassandra.connection.host", "localhost");

    val cluster = Cluster.builder().addContactPoint(cassandra_host).build()
    val session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS twitter WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    session.execute("CREATE TABLE IF NOT EXISTS twitter.tweet_stream (id int, name text, text text, word text, ts timestamp, count int, PRIMARY KEY(word, ts)) ")
    //session.execute("TRUNCATE Tweet")

    // Set kafka parameters for streaming context
    val line = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topic, kafkaParams)
    )

    val streaming_tweets = line.map(_.value) // split tweet into lines
      .flatMap(_.split(" ")) // split tweet into words
      //.flatMap(_.split(","))
      .filter(w=>w.length()>0) // remove any empty words caused by double spaces
      .map(w=>(w, 1L)).reduceByKey(_+_) // count by word line for each incoming RDD
      .map({case (w,c) => (w,new Date().getTime,c)}) // add the current timestamp saveToCassandra("Tweet,count")})
      .filter(lambda w: '#' in w).map(lambda x: (x, 1))

    streaming_tweets.print()

    streaming_tweets.foreachRDD(rdd => {
      println("------------------------------------------")
      //if (rdd.collect().size == 0)
        //println("NOTHING YET")
      //else{
       // println("size ="+rdd.collect().size)
      //}
      rdd.saveToCassandra("twitter", "tweet_stream")
    })
    ssc.start() // start the streaming context
    ssc.awaitTermination() // block while the context is running (until it's stopped by the timer)
    ssc.stop()

    //val c = CassandraConnector(sc.getConf)
    //val query = "INSERT INTO twitter.tweet_stream(id,text) VALUES ("+id+",'"+(rdd.collect()(0))+"')"

    println(query)
    c.withSessionDo(session => session.execute(query))
    // Get results using Spark SQL
    val rdd1 = sc.cassandraTable("twitter", "tweet_stream")
    rdd1.take(100).foreach(println)
    sc.stop()
  }
}
