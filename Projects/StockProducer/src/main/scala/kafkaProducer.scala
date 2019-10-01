import java.util.concurrent.LinkedBlockingDeque
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}
import twitter4j._
import twitter4j.conf.ConfigurationBuilder

object kafkaProducer {

  def main(args: Array[String]): Unit = {
    val queue = new LinkedBlockingDeque[Status](1000)
    val consumerKey = "vRODXfC4vv3j75KFlPXPDeCh1"
    val consumerSecret = "hG5pLRm79HYb0x1s6U8dgrZtFlifMyfk3iqQJOKx1kMYoGfBL4"
    val accessToken = "33548073-cKhmwn2iH0Z6RPOP8AZ1Vrc6z379nqbNcYaa4T31P"
    val accessTokenSecret = "iwAHC8VZbqQSwAQ492Vks4dKYoNUX0Et1W3FHba2uFzaC"

    val topicName = "Israel"
    val keywords = "Trump"

    val confBuild = new ConfigurationBuilder()

    confBuild.setDebugEnabled(true)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessTokenSecret)
      .setOAuthConsumerSecret(consumerSecret)
      .setOAuthConsumerKey(consumerKey)

    val stream = new TwitterStreamFactory(confBuild.build()).getInstance()

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

    stream.addListener(listener)

    val query = new FilterQuery(keywords)

    stream.filter(query)

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

        for(hashtagEntity <- status.getHashtagEntities){

          println("Tweet" + status + " \nHashtag" + hashtagEntity.getText)

          producer.send(new ProducerRecord[String,String](
            topicName,
            (count+=1).toString,
            status.getText))// Get the lines, split them into words, count the words and print


        }
      }
    }
  }

}