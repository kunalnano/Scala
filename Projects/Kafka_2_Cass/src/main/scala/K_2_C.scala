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

object K_2_C extends App {

  // kafka message
  case class Message(service: String, msg: String)

  // cassandra keeps Log
  case class Log(id: String = UUID.randomUUID.toString, service: String, msg: String, timestamp: Long = System.currentTimeMillis() / 1000)

  // we need to actor system and actor materializer
  // to execute(materialize in akka stream terms) we need materializer.
  // materializer is a special tool that runs streams
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val context = system.dispatcher

  // cassandra cluster
  val cluster = Cluster.builder().addContactPoint("localhost").withPort(9042).build()
  implicit val session = cluster.connect("k2c")

  // parse json messages comes from kafka
  import spray.json.DefaultJsonProtocol._
  implicit val messageFormat: JsonFormat[Message] = jsonFormat2(Message)

  // kafka consumer source
  // topic - 'log'
  // consumer group - 'log-group'
  val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
    .withBootstrapServers("localhost:9092")
    .withGroupId("log-group")
  val source = Consumer.committableSource(consumerSettings, Subscriptions.topics("Israel"))

  // cassandra sink to save log
  // keyspace - k2c
  // table - logs
  val sink = {
    val statement = session.prepare(s"INSERT INTO k2c.logs(id, service, msg, timestamp) VALUES (?, ?, ?, ?)")

    // we need statement binder to convert scala case class object types into java types
    val statementBinder: (Log, PreparedStatement) => BoundStatement = (l, ps) =>
      ps.bind(l.id: java.lang.String, l.service: java.lang.String, l.msg: java.lang.String, l.timestamp: java.lang.Long)

    // parallelism defines no of concurrent queries that can execute to cassandra
    CassandraSink[Log](parallelism = 10, statement = statement, statementBinder = statementBinder)
  }

  // flow to map kafka message which comes as JSON string to Message
  val toMessageFlow = Flow[CommittableMessage[String, String]]
    .map(p => p.record.value().parseJson.convertTo[Message])

  // flow to map Message to Log
  val toLogFlow = Flow[Message]
    .map(p => Log(service = p.service, msg = p.msg))

  // stream graph
  source
    .via(toMessageFlow)
    .via(toLogFlow)
    .runWith(sink)

}