package processors.statefull



import org.apache.kafka.streams.scala._

import java.util.Properties
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology, scala}
import org.apache.kafka.streams.scala.kstream._
import schemas.{Facture, OrderLine}
import serdes.{BytesDeserializer, BytesSerdes, BytesSerializer, JSONDeserializer, JSONSerializer}
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.kstream.Printed


object ReduceProcessor extends App {


  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.Serdes.String
  import org.apache.kafka.streams.scala.Serdes._


  implicit val jsonSerdes : Serde[Facture]= Serdes.serdeFrom(new  JSONSerializer[Facture], new JSONDeserializer)
  implicit val consumed : Consumed[String, Facture] = Consumed.`with`(Serdes.String(), jsonSerdes)
  implicit val produced : Produced[String, Facture] = Produced.`with`(Serdes.String(),jsonSerdes)

  val props: Properties = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "reduce-processor1")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")


  val str : StreamsBuilder = new StreamsBuilder()
  val kstrFacture : KStream[String, Facture] = str.stream[String, Facture]("factureBinJSO")


  val kCA = kstrFacture
    .map((k, f) => ("1", f.total))  // calcul du chiffre d'affaire total
    //.mapValues(f => f.total)    // calcul du chiffre d'affaire par clé
    .groupBy((k, t) => k)(Grouped.`with`(String, Double ))
    .reduce((aggValue, currentValue) => aggValue + currentValue )(Materialized.as("ReducerStore"))


    kCA.toStream.print(Printed.toSysOut().withLabel("Chiffre d'affaire global"))




  val topologie : Topology = str.build()
  val kkStream : KafkaStreams = new KafkaStreams(topologie, props)
  kkStream.start()

  sys.ShutdownHookThread {
    kkStream.close()
  }



}
