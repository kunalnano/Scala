import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils}
import pl.pcejrowski.SentimentAnalyzer_cl


object kafkaConsumer {

  def main(args: Array[String]): Unit = {

    // Create Spark configuration - Set App Name and Set Master (Computer location(Cores))
    val conf = new SparkConf().setAppName("TwitterKD").setMaster("local[*]")

    // Create a driver for Spark
    val ssc = new StreamingContext(conf, Seconds(5))

    // Set Topic for Broker
    val topics = List("Israel").toSet

    // Set parameters for Kafka
    val kafkaParams = Map(
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark-streaming-something",
      "auto.offset.reset" -> "earliest")

    // Set DStream parameters

    val line = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )
     // Reading input into RDD
    val temp = line.map(record => record.value().toString)
    val _line = temp.flatMap(_.split(","))

    // Printing RDD output
    _line.print
    ssc.start()
    ssc.awaitTermination()

    // Importing and applying a streaming filter for sentiment analysis

    val sent = new SentimentAnalyzer_cl()

    sent.mainSentiment((_line).toString)

  }
}
