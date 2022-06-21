package processors.statefull

import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.kstream.Printed
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import schemas.Facture
import schemas._
import serdes.{JSONDeserializer, JSONSerializer}

import java.util.Properties



object AggregateProcessor extends App {


  import org.apache.kafka.streams.scala.Serdes.{String, _}
  import org.apache.kafka.streams.scala.Serdes._


  implicit val jsonSerdes : Serde[Facture]= Serdes.serdeFrom(new  JSONSerializer[Facture], new JSONDeserializer)
  implicit val consumed : Consumed[String, Facture] = Consumed.`with`(Serdes.String(), jsonSerdes)
  implicit val produced : Produced[String, Facture] = Produced.`with`(Serdes.String(),jsonSerdes)

  val props: Properties = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "aggregate-processor")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")


  val str : StreamsBuilder = new StreamsBuilder()
  val kstrFacture : KStream[String, Facture] = str.stream[String, Facture]("factureBinJSO")


  val kCA = kstrFacture
    .map((k, f) => ("1", f.total))
    .groupBy((k, t) => k)(Grouped.`with`(String, Double ))
    .aggregate(0D)((key, newValue, aggValue) => aggValue + newValue)(Materialized.as("AggregateStore")(String, Double))


  kCA.toStream.print(Printed.toSysOut().withLabel("Chiffre d'affaire global"))

/*
  // méthode 2
  val kCA2 = kstrFacture
    .map((k, f) => ("1", f))
    .groupBy((k, t) => k)(Grouped.`with`(String, Double ))
    .aggregate[Double](0)((k, newFacture, aggFacture) => newFacture.total + aggFacture)(Materialized.as("AggregateStore-2"))



  kCA2.toStream.print(Printed.toSysOut().withLabel("Chiffre d'affaire global - M2"))
*/


/*
  // calcul du chiffre d'affaire moyen
  val kCA3 = kstrFacture
    .map((k, f) => ("1", f))
    .groupBy((k, t) => k)
    .aggregate[Facture](Facture("", "", 0, 0D, OrderLine("", "", "", "", 0D, 0D, 0)))(
      (key, newFacture, aggFacture) => Facture(newFacture.factureid, "", newFacture.quantite + aggFacture.quantite,
        newFacture.total + aggFacture.total, OrderLine("", "", "", "", 0D, 0D, 0)))
*/

  val topologie : Topology = str.build()
  val kkStream : KafkaStreams = new KafkaStreams(topologie, props)
  kkStream.start()

  sys.ShutdownHookThread {
    kkStream.close()
  }



}
