import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object WordCount extends App {
  val spark = SparkSession.builder
      .master("local[*]")
      .appName("Spark Word Count")
      .getOrCreate()

  val lines = spark.sparkContext.textFile("/Users/kunalsharma/Downloads/pride_and_prejudice.txt")

  val counts = lines
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

  counts.saveAsTextFile("/Users/kunalsharma/Downloads/wordcount")
}


