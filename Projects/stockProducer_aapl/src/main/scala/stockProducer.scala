
import java.util.concurrent.LinkedBlockingDeque
import org.apache.spark.streaming
import org.apache.spark
import java.util.Properties
import org.apache.spark.streaming.kafka._
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}
import org.apache.spark.sql.SparkSession
import twitter4j._
//import twitter4j.conf.ConfigurationBuilder
import org.apache.log4j.BasicConfigurator
import org.apache.spark.sql.streaming.DataStreamReader

object stockProducer {
  def main(args: Array[String]): Unit = {

    BasicConfigurator.configure()
    val queue = new LinkedBlockingDeque[Status](1000)

    val topicName = "stocks"

    val url = "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=AAPL&interval=1min&apikey=3FUAEVGL9PE0WCAT"
    val stream = scala.io.Source.fromURL(url).mkString

   // val session = SparkSession.builder().appName("test").master("local").getOrCreate()
   // val stream_rdd = spark.SparkContext.getOrCreate().parallelize(url)

    val path = "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=AAPL&interval=1min&apikey=3FUAEVGL9PE0WCAT"
    // Read data continuously from an API
    // val inputDF = spark.read.json(path)

    val listener = new StatusListener {

      override def onStatus(status: Status): Unit = {

        queue.offer(status)

      } //Defining the queue

      override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = {

        println("Got a status deletion notice id:"

          + statusDeletionNotice.getStatusId)

      }//end of onDeletionNotice override

      override def onTrackLimitationNotice(numberOfLimitedStatuses: Int): Unit = {

        println("Got track limitation notice:"

          + numberOfLimitedStatuses)

      }//end of onTrackLimitationNotice override

      override def onScrubGeo(userId: Long, upToStatusId: Long): Unit = {

        println("Got scrub_geo event userId:"

          + userId

          + "upToStatusId:"

          + upToStatusId)

      }//end of onScrubGeo override
      override def onStallWarning(warning: StallWarning): Unit = {

        println("Got stall warning:"

          + warning)

      }// end of onStallWarning override

      override def onException(ex: Exception): Unit = {

        ex.printStackTrace()

      }// end of onException override
    }

    /////// KAFKA CONFIGURATION //////

    val properties = new Properties()
    properties.put("metadata.broker.list","localhost:9092")
    properties.put("bootstrap.servers","localhost:9092")
    properties.put("ack","all")
    properties.put("retries","0")
    properties.put("batch.size","16384")
    properties.put("buffer.memory","33554432")
    properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](properties)

    var count: Int = 0

    while(true){

      val status = queue.poll()

      if(status == null){

        //Thread.sleep(100)

      } else {

          producer.send(new ProducerRecord[String,String](
            topicName,
            (count+=1).toString,
            status.getText))
        }
      println(stream)
      }
    }


}

