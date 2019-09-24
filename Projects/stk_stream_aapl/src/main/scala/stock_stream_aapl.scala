import scala.io.Source
import org.apace.spark

object stock_stream_aapl {
  def main(args: Array[String]): Unit = {

    val url = ("https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=AAPL&interval=1min&apikey=3FUAEVGL9PE0WCAT")
    val result = scala.io.Source.fromURL(url).mkString

    val stk_ds = spark.read
  }

}
