import org.apache.spark.SparkContext
import org.apache.spark.sql
import scala.io.Source

object df_airbnb{

  def main(args: Array[String]): Unit = {

    val lines = Source.fromFile("/Users/kunalsharma/Downloads/AB_NYC_2019.csv").getLines.mkString.split("\\s+")

  }

}


