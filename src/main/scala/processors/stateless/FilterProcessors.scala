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
import org.apache.kafka.streams.scala.Serdes.Double


object FilterProcessors {

  val props: Properties = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "map-processor")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")


  def main(args: Array[String]): Unit = {
    run()
  }


  def topologie() : Topology = {

    implicit val jsonSerdes: Serde[Facture] = Serdes.serdeFrom(new JSONSerializer[Facture], new JSONDeserializer)
    implicit val consumed: Consumed[String, Facture] = Consumed.`with`(Serdes.String(), jsonSerdes)
    implicit val produced: Produced[String, Double] = Produced.`with`(Serdes.String(), Double)


    val str: StreamsBuilder = new StreamsBuilder()
    val kstrFacture: KStream[String, Facture] = str.stream[String, Facture]("factureBinJSO")

    //utilisation de Filter() / FilterNot()
    val kstrTotal : KStream[String, Double] = kstrFacture.mapValues(f => f.orderLine.numunits * f.orderLine.unitprice)
    //val kstrFilt = kstrTotal.filter((_,t) => t > 2000)

    //val kstrFiltNot = kstrTotal.filterNot((_,t) => t > 2000)

    kstrTotal.to("topic-test")(produced)

    kstrTotal.print(Printed.toSysOut().withLabel("résultat du CA"))


    val topologie: Topology = str.build()

    topologie


  }


  def run() : Unit = {

    val kkStream: KafkaStreams = new KafkaStreams(topologie(), props)
    kkStream.start()

    sys.ShutdownHookThread {
      kkStream.close()
    }

  }




}
