package processors.statefull

import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.kstream.{Printed, SessionWindows, TimeWindows, Windowed}
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import schemas.Facture
import schemas._
import serdes.{JSONDeserializer, JSONSerializer}

import java.time.{Duration, ZoneOffset}
import java.util.Properties


object WindowingProcess extends App {


  import org.apache.kafka.streams.scala.Serdes.{String, _}
  import org.apache.kafka.streams.scala.Serdes._


  implicit val jsonSerdes : Serde[Facture]= Serdes.serdeFrom(new  JSONSerializer[Facture], new JSONDeserializer)
  implicit val consumed : Consumed[String, Facture] = Consumed.`with`(new FactureTimeStamExtractor)(Serdes.String(), jsonSerdes)
  implicit val produced : Produced[String, Facture] = Produced.`with`(Serdes.String(),jsonSerdes)


  val props: Properties = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "windowingProcess-intermediaire")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, "org.apache.kafka.streams.processor.WallclockTimestampExtractor")
  props.put("message.timestamp.type", "LogAppendTime")


  val str : StreamsBuilder = new StreamsBuilder()
  val kstrFacture : KStream[String, Facture] = str.stream[String, Facture]("factureBinJSO")(consumed)



  val kCA = kstrFacture
    .map((k, f) => ("1", f.total))
    .groupBy((k, t) => k)(Grouped.`with`(String, Double ))
    .aggregate[Double](0D)((key, newValue, aggValue) => aggValue + newValue)(Materialized.as("AggregateStore5")(String, Double))


 // kCA.toStream.print(Printed.toSysOut().withLabel("Chiffre d'affaire global"))


  // fenêtres fixes
  val kCA_ff : KTable[Windowed[String], Long] = kstrFacture
    .map((k, f) => (k, f.total))
    .groupBy((k, t) => k)(Grouped.`with`(String, Double ))
    .windowedBy(TimeWindows.of(Duration.ofSeconds(15)))
    .count()(Materialized.as("FenetresFixes")(String, Long))

  //kCA_ff.toStream.print(Printed.toSysOut().withLabel("Comptage Fenêtres Fixes"))


  // fenêtres glissantes
  val kCA_fg : KTable[Windowed[String], Long] = kstrFacture
    .map((k, f) => (k, f.total))
    .groupBy((k, t) => k)(Grouped.`with`(String, Double ))
    .windowedBy(TimeWindows.of(Duration.ofSeconds(15)).advanceBy(Duration.ofSeconds(3)))
    .count()(Materialized.as("FenetresGlissantes")(String, Long))

  //kCA_fg.toStream.print(Printed.toSysOut().withLabel("Intervalle Fenêtres Glissantes"))


  // Session
  val kCA_s : KTable[Windowed[String], Long] = kstrFacture
    .map((k, f) => (k, f.total))
    .groupBy((k, t) => k)(Grouped.`with`(String, Double ))
    .windowedBy(SessionWindows.`with`(Duration.ofMinutes(5)))
    .count()(Materialized.as("Session")(String, Long))

  //kCA_s.toStream.print(Printed.toSysOut().withLabel("Implementation Intervalle de Session"))


  kCA_s.toStream.foreach {
    (key, value) => println(
      s"clé de la fenêtre : ${key}, " +
        s"clé du message : ${key.key()}, " +
        s"debut : ${key.window().startTime().atOffset(ZoneOffset.UTC)}, " +
        s"fin : ${key.window().endTime().atOffset(ZoneOffset.UTC)}, " +
        s"valeur : ${value}")

  }




  /*
    // méthode 2
    val kCA2 = kstrFacture
      .map((k, f) => ("1", f))
      .groupBy((k, t) => k)(Grouped.`with`(String, Double))
      .aggregate[Double](0)((k, newFacture, aggFacture) => newFacture.total + aggFacture)(Materialized.as("AggregateStore - M2")(String, Double))


    kCA2.toStream.print(Printed.toSysOut().withLabel("Chiffre d'affaire global - M2"))
  */



/*
  // calcul du chiffre d'affaire moyen
  // Calcul Chiffre d'affaire moyen réalisé toutes les 15 secondes en fenêtres fixes
  val kCA3 = kstrFacture
    .map((k, f) => ("1", f))
    .groupBy((k, t) => k)(Grouped.`with`(String, Double))
    .windowedBy(TimeWindows.of(Duration.ofSeconds(15)))
    .aggregate[Facture](Facture("", "", 0, 0D, OrderLine("", "", "", "", 0D,0D, 0)))(
      (key, newFacture, aggFacture) =>
        Facture(newFacture.factureid, "",
          newFacture.quantite + aggFacture.quantite,
          newFacture.total + aggFacture.total,
          OrderLine("", "", "", "", 0D,0D, 0))
    )(Materialized.as("AggregateStore - M2")(String, Serde[Facture]))
    .mapValues(f => (f.quantite, f.total, f.total/f.quantite))

  kCA3.toStream.print(Printed.toSysOut().withLabel("Panier moyen du consommateur - M3"))
*/


  val topologie : Topology = str.build()
  val kkStream : KafkaStreams = new KafkaStreams(topologie, props)
  kkStream.start()

  sys.ShutdownHookThread {
    kkStream.close()
  }



}
