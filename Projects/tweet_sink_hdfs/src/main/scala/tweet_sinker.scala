import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.twitter.TwitterUtils


object tweet_sinker {

  val conf = new SparkConf().setAppName("hdfs_sink").setMaster("local[*]")
  val ssc = new StreamingContext(conf, Seconds(5))
  val topics = List("Israel").toSet

  val kafkaParams = Map( //Parameters to connect to Kafka
    "bootstrap.servers" -> "localhost:9092", //
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "spark-streaming-something",
    "auto.offset.reset" -> "earliest"
    //    val config = new SparkConf().setAppName("twitter-stream-sentiment")
    //    val sc = new SparkContext(config)
    //    sc.setLogLevel("WARN")
  )
  val line = KafkaUtils.createDirectStream[String, String](
    //Parameter for Streaming Context
    ssc,
    PreferConsistent,
    ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
  )

  // From Spark 2.0.0 to onward use SparkSession (managing Context)
  val spark = SparkSession.builder.appName("twitter-sentiment-analysis").getOrCreate()
  val sc = spark.sparkContext
  // Create Spark Streaming Context

  // Twitter App API Credentials - underlying twitter4j Library
  System.setProperty("twitter4j.oauth.consumerKey", "HEPneO1YX0o4oiKD8PQt1BCwv")
  System.setProperty("twitter4j.oauth.consumerSecret", "RCEshgBhhjsSFWT4xiS2tqsbrYgRcthOJHwcx1Gcji05WykSMs")
  System.setProperty("twitter4j.oauth.accessToken", "1172222860980625418-8ymnbJtJLFweKLl3Pi4gqlEpXJ9c40")
  System.setProperty("twitter4j.oauth.accessTokenSecret", "OWMEAAZcEvItDhSfuxIdFMN06jjYJbbjf9uCFF2s5EmlY")
  //val filters = Seq("iphone", "samsung galaxy,samsung note")
  //    val twitterStream = TwitterUtils.createStream(ssc,None,filters)
  val twitterStream = TwitterUtils.createStream(ssc, None).toString

   val rdd = sc.parallelize(twitterStream)
   rdd.saveAsObjectFile ("hdfs:///test1/")


}
