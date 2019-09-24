import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.storage.StorageLevel
import scala.io.Source
import scala.collection.mutable.HashMap
import java.io.File

object tweetSentimentAnalysis {
  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println("Usage: TwitterPopularTags <consumer key> <consumer secret>" + "<access token> <access token secret> |<filters>|")
      System.exit(1)
    }
    StreamingExamples.setStreamingLogLevels()

    //Passing Twitter keys and tokens as arguments for authorization

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)

    // Set system properties so that twitter4j library is used by twitter stream
    // use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerkey", consumerKey)
    System.setProperty("twitter.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter.oauth.accessToken", accessToken)
    System.setProperty("twitter.oauth.accessTokenSecret", accessTokenSecret)

    val sparkConf = new SparkConf().setAppName("Sentiments").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val stream = TwitterUtils.createStream(ssc, None, filters)

    // Input DStream transformation using flatMap

    val tags = stream.flatMap { status => status.getHashtagEntities.map(_.getText)}

    // RDD Transformation using sortBy and then map function

    tags.countByValue().foreachRDD { rdd => val now = org.joda.time.DateTime.now()
      rdd
        .sortBy(_._2)
        .map(x => now)

      // Saving our output at ~/twitter/$now

        .saveAsTextFile(s"~/twitter/$now")
    }

    // DStream transformation using filter and map functions

    val tweets = stream.filter { t =>
      val tags = t.getText.split("").filter(_.startsWith("#")).map(_.toLowerCase)
      tags.exists { x => true }

      val data = tweets.map { status =>
        val sentiment = SentimentAnalysisUtils.detectSentiment(status.getText)
        val tagss = status.getHashTagEntities.map(_.getText.toLowerCase)
        (status.getText, sentiment.toString, taggs.toString())
      }

      data.print()

      // Saving our output at ~/ with filenames starting like twitters

      data.saveAsTextFiles("~/twitters", "20000")

      ssc.start()
      ssc.awaitTermination()
    }




  }

}
