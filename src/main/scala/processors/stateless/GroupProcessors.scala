package processors.stateless



import org.apache.kafka.streams.scala._

import java.util.Properties
import org.apache.kafka.streams.{StreamsConfig, Topology}
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.KafkaStreams
import schemas.{Facture, OrderLine}
import serdes.{JSONDeserializer, JSONSerializer}
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.kstream.Printed
import org.apache.kafka.streams.scala.Serdes._


object GroupProcessors extends App {

  implicit val jsonSerdes: Serde[Facture] = Serdes.serdeFrom(new JSONSerializer[Facture], new JSONDeserializer)
  implicit val consumed: Consumed[String, Facture] = Consumed.`with`(String, jsonSerdes)
  implicit val produced: Produced[String, Facture] = Produced.`with`(String, jsonSerdes)

  val props: Properties = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "group-processor")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

  val str: StreamsBuilder = new StreamsBuilder()
  val kstrFacture: KStream[String, Facture] = str.stream[String, Facture]("factureBinJSO")
  val kstrTotal : KStream[String, Double] = kstrFacture.mapValues(f => f.orderLine.numunits * f.orderLine.unitprice)

  // selectKey()
  val newKeys = kstrTotal.selectKey((k, t) => k.toUpperCase())
  newKeys.print(Printed.toSysOut().withLabel("Select keys"))

  // GroupByKey()
  val kstGroupKeys = newKeys.groupByKey(Grouped.`with`(String, Double))

  //GroupBy()
  val kstrGroupBy = newKeys.groupBy((k, v) => v)(Grouped.`with`(Double, Double))


  val topologie: Topology = str.build()
  val kkStream: KafkaStreams = new KafkaStreams(topologie, props)
  kkStream.start()

  sys.ShutdownHookThread {
    kkStream.close()
  }


}
