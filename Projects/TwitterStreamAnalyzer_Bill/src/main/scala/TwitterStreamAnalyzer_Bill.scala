import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.twitter._

object TwitterStreamAnalyzer_Bill {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Bill").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))
    val topics = List("Israel").toSet // You have two topics; "israel" and "manny"

    val kafkaParams = Map( //Parameters to connect to Kafka
      "bootstrap.servers" -> "localhost:9092", //This's Al's IP. if you want local; "bootstrap.servers" -> "localhost:9092",
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
    val twitterStream = TwitterUtils.createStream(ssc, None)
    val englishTweets = twitterStream.filter(_.getLang == "en")
    englishTweets.map(_.getText).print()


    // DStream Created using Stanford NLP Library
    // English Model Used (stanford-corenlp-3.9.1-models-english.jar)
    val dataDS = englishTweets.map { tweet =>
      val sentiment = NLPManager.detectSentiment(tweet.getText)
      val tags = tweet.getHashtagEntities.map(_.getText.toLowerCase)
      //      println("(" + tweet.getText + " | " + sentiment.toString + " | " + tags)
      (tweet.getText, sentiment.toString, tags)
    }

    val sqlContex = spark.sqlContext

    //    var dataRDD : org.apache.spark.rdd.RDD[(String,String,Array[String])] = sc.emptyRDD
    dataDS.cache().foreachRDD(rdd => {
      val df = spark.createDataFrame(rdd)
      df.show()
      df.createOrReplaceTempView("sentiments")
      sqlContex.sql("select * from sentiments limit 20").show()
      // Combine RDDs
      //      dataRDD.union(rdd)
    })

    /*
    // Convert DataFrame into SQL Table
    val df = spark.createDataFrame(dataRDD)
    df.show()
    df.createOrReplaceTempView("sentiments")
    sqlContex.sql("select * from sentiments limit 30").show()
    */
    val tmp = line.map(record => record.value().toString)
    val _line = tmp.flatMap(_.split(",")) // This line is Dstream

    ssc.start()
    ssc.awaitTermination()
    //    spark.wait()

  }

}
