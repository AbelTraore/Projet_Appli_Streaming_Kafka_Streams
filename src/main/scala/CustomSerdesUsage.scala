import org.apache.kafka.streams.scala._

import java.util.Properties
import org.apache.kafka.streams.{StreamsConfig, Topology}
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.KafkaStreams
import HelloWorld_KafkaStreams._
import schemas.{Facture, OrderLine}
import serdes.{BytesDeserializer, BytesSerdes, BytesSerializer, JSONDeserializer, JSONSerializer}
import org.apache.kafka.common.serialization.{Serde, Serdes}


object CustomSerdesUsage extends App {

  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.Serdes.String
  import org.apache.kafka.streams.scala.Serdes.Integer


  //1ère méthode d'utilisation des custum serdes
  implicit val bytesSerdes = new BytesSerdes[Facture]
  implicit val bytesOrdersSerdes = new BytesSerdes[OrderLine]


  //2ème méthode d'utilisation des custum serdes
  implicit val factureSerdes: Serde[Facture] = Serdes.serdeFrom[Facture](new BytesSerializer[Facture], new BytesDeserializer[Facture])
  implicit val consumed : Consumed[String, Facture] = Consumed.`with`(Serdes.String(), factureSerdes)
  implicit val produced : Produced[String, Facture] = Produced.`with`(Serdes.String(),factureSerdes)
  implicit val jsonSerdes : Serde[Facture]= Serdes.serdeFrom(new  JSONSerializer[Facture], new JSONDeserializer)


    val str : StreamsBuilder = new StreamsBuilder()
    val kstr : KStream[String, Facture] = str.stream[String, Facture]("streams_app")
    val kstrMaj : KStream[String, Int] = kstr.mapValues(v => v.quantite)
    kstrMaj.to("streams_app_upper")

    val topologie : Topology = str.build()
    val kkStream : KafkaStreams = new KafkaStreams(topologie, getParams("localhost:9092"))
    kkStream.start()

    sys.ShutdownHookThread {
      kkStream.close()
    }




}