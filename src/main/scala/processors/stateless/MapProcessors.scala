package processors.stateless

import org.apache.kafka.streams.scala._

import java.util.Properties
import org.apache.kafka.streams.{StreamsConfig, Topology}
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.KafkaStreams
import schemas.{Facture, OrderLine}
import serdes.{JSONDeserializer, JSONSerializer}
import org.apache.kafka.common.serialization.{Serde, Serdes}


object MapProcessors extends App {

  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.Serdes.String
  import org.apache.kafka.streams.scala.Serdes.Integer


  implicit val jsonSerdes: Serde[Facture] = Serdes.serdeFrom(new JSONSerializer[Facture], new JSONDeserializer)
  implicit val consumed: Consumed[String, Facture] = Consumed.`with`(Serdes.String(), jsonSerdes)
  implicit val produced: Produced[String, Facture] = Produced.`with`(Serdes.String(), jsonSerdes)

  val props: Properties = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "map-processor")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

  val str: StreamsBuilder = new StreamsBuilder()
  val kstrFacture: KStream[String, Facture] = str.stream[String, Facture]("factureBinJSO")

  //utilisation d'un MapValue()
  val kstrTotal : KStream[String, Double] = kstrFacture.mapValues(f => f.orderLine.numunits * f.orderLine.unitprice)

  //utilisation d'un Map() - cause les données à être marqué pour re-partitionnement
  val kstrTotal2 : KStream[String, Double] = kstrFacture.map((k,f) => (k.substring(2), f.orderLine.numunits * f.orderLine.unitprice))

  val topologie: Topology = str.build()
  val kkStream: KafkaStreams = new KafkaStreams(topologie, props)
  kkStream.start()

  sys.ShutdownHookThread {
    kkStream.close()
  }

}