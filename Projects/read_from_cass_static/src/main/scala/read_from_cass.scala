import com.datastax.spark.connector._
import org.apache.spark.{SparkConf, SparkContext}

object read_from_cass {

  val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1")
  val sc = new SparkContext(conf)
  val rdd = sc.cassandraTable("countries", "countries")
  val file_collect = rdd.collect().take(1)
  println(file_collect)
}
