import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.apache.commons.configuration.ConfigurationFactory.ConfigurationBuilder

import java.util.Properties
import java.util.concurrent.LinkedBlockingDeque



object Producer {
  def main(args: Array[String]): Unit = {

    val queue = new LinkedBlockingDeque[Status](capacity = 1000)
    val consumerKey = "5SSqL8nhHhhcTzqMDLaz8IZek"
    val secretConsumerKey = "gqZJGxZgp6Dy4quc2VdeSFLKMmBf7DPzReM7EYxoeIDQ7QW6lZ"
    val accessToken = "33548073-xQCy86AYVnRXFGvrXA1NeD1dP1MOXkslRSvPOVWl2"
    val accessTokenSecret = "gyCJfouy0OX40qCrrvYZ7eHZiOHDjnTwl4R8Xi14Wzu8V"

    val topicName = "Atlanta"
    val keyword = "Atlanta"

    val confBuild = new ConfigurationBuilder()

    confBuild.setDebugEnabled(true)
      .setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(secretConsumerKey)
      .setOAuthAccessToken(accessToken)
      .setOAuthTokenSecret(accessTokenSecret)

    val stream = new TwitterStreamFactory(confBuild.build().getInstance())

    val listener = new StatusListener {
      override def onStatus(status: Status): Unit = {
        queue.offer(status)
      }

      override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = {

        println("Got a status deletion notice id:"

          + statusDeletionNotice.getStatusId)

      } //end of onDeletionNotice override


      override def onTrackLimitationNotice(numberOfLimitedStatuses: Int): Unit = {

        println("Got track limitation notice:"

          + numberOfLimitedStatuses)

      } //end of onTrackLimitationNotice override


      override def onScrubGeo(userId: Long, upToStatusId: Long): Unit = {

        println("Got scrub_geo event userId:"

          + userId

          + "upToStatusId:"

          + upToStatusId)

      } //end of onScrubGeo override


      override def onStallWarning(warning: StallWarning): Unit = {

        println("Got stall warning:"

          + warning)

      } // end of onStallWarning override


      override def onException(ex: Exception): Unit = {

        ex.printStackTrace()

      } // end of onException override

    } // End of listener


    stream

      .addListener(listener)

    // filter the search by keywords

    val query = new FilterQuery(keyword)

    stream.filter(query)

    //kafka producer properties

    val properties = new Properties()

    properties.put("metadata.broker.list", "localhost:9092")
    properties.put("bootstrap.servers", "localhost:9092")
    properties.put("ack", "all")
    properties.put("retries", "0")
    properties.put("batch.size", "16384")
    properties.put("linger.ms", "1")
    properties.put("buffer.memory", "33554432")
    properties.put("key.serializer", "org.apache.kafka.common.serializer.StringSerializer")

    val producer = new KafkaProducer[String, String](properties)

    var count: Int = 0

    while (true) {

      val status = queue.poll()

      if (status == null) {
        //No new tweets - wait
      } else {
        //there are tweets
        for (hashtagEntity <- status.getHashtagEntities) {
          println("Tweet: " + status + "\nHashtag" + hashtagEntity.getText)

          producer.send(new ProducerRecord[String, String]
          (
            topicName,
            (count += 1).toString,
            status.getText
          )
          )
        }
      }

    }
  }

}
