package processors.stateless

import org.apache.kafka.streams.scala._

import java.util.Properties
import org.apache.kafka.streams.{StreamsConfig, Topology}
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.KafkaStreams
import schemas.{Facture, OrderLine}
import serdes.{JSONDeserializer, JSONSerializer}
import org.apache.kafka.common.serialization.{Serde, Serdes}


object FlatMapProcessors extends App {

  implicit val jsonSerdes: Serde[Facture] = Serdes.serdeFrom(new JSONSerializer[Facture], new JSONDeserializer)
  implicit val consumed: Consumed[String, Facture] = Consumed.`with`(Serdes.String(), jsonSerdes)
  implicit val produced: Produced[String, Facture] = Produced.`with`(Serdes.String(), jsonSerdes)

  val props: Properties = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "map-processor")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

  val str: StreamsBuilder = new StreamsBuilder()
  val kstrFacture: KStream[String, Facture] = str.stream[String, Facture]("factureBinJSO")
  val kstrProducts: KStream[String, Array[String]] = kstrFacture.flatMapValues(f => List(f.productName.split(" ")))


  val kstrTotal : KStream[String, Double] = kstrFacture.flatMapValues(f => List(f.orderLine.numunits * f.orderLine.unitprice, f.orderLine.numunits, f.orderLine.unitprice))

  val kstrTotal2 : KStream[String, Double] = kstrFacture.flatMap((k,f) => List(
    (k.toUpperCase(), f.orderLine.numunits * f.orderLine.unitprice),
    (k.substring(2, 1),f.orderLine.numunits),
    (k.substring(2, 2),f.orderLine.unitprice))
  )

  val kstrFilt = kstrTotal.filter((_,t) => t > 2000)

  val topologie: Topology = str.build()
  val kkStream: KafkaStreams = new KafkaStreams(topologie, props)
  kkStream.start()

  sys.ShutdownHookThread {
    kkStream.close()
  }


}
