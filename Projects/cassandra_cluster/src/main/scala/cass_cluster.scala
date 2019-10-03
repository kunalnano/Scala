
import java.util.UUID

import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.ActorMaterializer
import akka.stream.alpakka.cassandra.scaladsl.CassandraSink
import akka.stream.scaladsl.Flow
import com.datastax.driver.core.{BoundStatement, Cluster, PreparedStatement}
import org.apache.kafka.common.serialization.StringDeserializer
import spray.json.JsonFormat
import spray.json._

object cass_cluster {
  def main(args: Array[String]): Unit = {

    val cluster = Cluster.builder().addContactPoint("localhost").withPort(9042).build()
    implicit val session = cluster.connect("mystiko")

  }

}
