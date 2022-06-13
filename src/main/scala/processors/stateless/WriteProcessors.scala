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


object WriteProcessors extends App {

  implicit val jsonSerdes: Serde[Facture] = Serdes.serdeFrom(new JSONSerializer[Facture], new JSONDeserializer)
  implicit val consumed: Consumed[String, Facture] = Consumed.`with`(Serdes.String(), jsonSerdes)
  implicit val produced: Produced[String, Facture] = Produced.`with`(Serdes.String(), jsonSerdes)

  val props: Properties = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "write-processor")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

  val str: StreamsBuilder = new StreamsBuilder()
  val kstrFacture: KStream[String, Facture] = str.stream[String, Facture]("factureBinJSO")
  val kstrTotal : KStream[String, Double] = kstrFacture.mapValues(f => f.orderLine.numunits * f.orderLine.unitprice)

  // selectKey()
  val newKeys = kstrTotal.selectKey((k, t) => k.toUpperCase())
  newKeys.print(Printed.toSysOut().withLabel("Select keys"))

  // GroupByKey()
  val kstGroupKeys = newKeys.groupByKey(Grouped.`with`(Serdes.String(), Serdes.Double()))

  //GroupBy()
  val kstrGroupBy = newKeys.groupBy((k, v) => v)(Grouped.`with`(Serdes.String(), Serdes.Double()))

  // écriture dans un topic Kafka existant - opération finale
  newKeys.to("topic_test")(Produced.`with`(Serdes.String(), Serdes.Double()))

  // écriture dans un topic Kafka existant - opération non - finale
  val t = newKeys.through("topic_test")(Produced.`with`(Serdes.String(), Serdes.Double()))

  /*
  // disponible uniquement à partir de la version 2.6.0 de Kafka Streams.
  kstr.repartition(Repartitioned.numberOfPartitions(3).withName("nom_topic_interne"))
  t = through() => str.stream(t)
   */

  //transformation de KStreams en KTable
  val kaTable = str.table[String, Facture]("factureBinJSO")(Consumed.`with`(Serdes.String(), jsonSerdes))

  val topologie: Topology = str.build()
  val kkStream: KafkaStreams = new KafkaStreams(topologie, props)
  kkStream.start()

  sys.ShutdownHookThread {
    kkStream.close()
  }



}
