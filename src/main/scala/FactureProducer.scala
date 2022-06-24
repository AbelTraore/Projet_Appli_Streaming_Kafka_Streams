import java.util.Properties
import org.apache.kafka.clients.producer.{ProducerRecord, _}
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import org.apache.kafka.common.serialization.BytesSerializer
import org.apache.kafka.clients.producer._
import serdes._
import schemas._

object FactureProducer extends App {

  val props = new Properties()
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[JSONSerializer[Facture]])
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

  val facture1 = List(Facture("a326", "téléviseur LG 3A Nano", 3, 3350.75, OrderLine("34e", "45i", "20/09/2010", "20/09/2010", 15.00, 700, 10)),
    Facture("a327", "téléviseur LG ", 3, 3350.75, OrderLine("34e", "45a", "20/09/2010", "20/09/2010", 15.00, 700, 10)),
    Facture("a328", "téléviseur LG 3A Nano", 3, 3350.75, OrderLine("34b", "45i", "20/09/2010", "20/09/2010", 15.00, 700, 10)),
    Facture("a329", "téléviseur LG 3A ", 1, 3350.75, OrderLine("34e", "45i", "21/09/2010", "20/09/2010", 15.00, 700, 10)),
    Facture("a330", "téléviseur LG Nano", 2, 3350.75, OrderLine("34e", "48i", "20/09/2010", "20/09/2010", 15.00, 700, 10)),
    Facture("a331", "téléviseur LG 2A Nano", 4, 3350.75, OrderLine("34e", "45i", "20/09/2010", "20/09/2010", 15.00, 700, 10))
  )


  val factureProducer = new KafkaProducer[String, Facture](props)


  facture1.foreach{
    e => factureProducer.send(new ProducerRecord[String, Facture]("factureBinJSO", e.factureid, e))
      Thread.sleep(3000)
  }

  println("rajout effectué avec succès !")

}
