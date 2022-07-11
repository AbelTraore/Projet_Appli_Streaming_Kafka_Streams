package processors.stateless

import org.apache.kafka.common.serialization.{DoubleDeserializer, StringDeserializer, StringSerializer}
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.Assert._
import org.junit.jupiter.api.{DisplayName, MethodOrderer, Order, TestMethodOrder}
import org.apache.kafka.streams.{StreamsConfig, TopologyTestDriver}
import org.apache.kafka.streams.test.{ConsumerRecordFactory, OutputVerifier}
import schemas.{Facture, OrderLine}
import serdes.{JSONDeserializer, JSONSerializer}

import java.util.Properties


@TestMethodOrder(classOf[MethodOrderer.OrderAnnotation])
class FiltersTopolgyTest {

  val props: Properties = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "map-processor-test")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

  val stringSer = new StringSerializer
  val factureSer = new JSONSerializer[Facture]

  val filterTopology = FilterProcessors
  val testDriver = new TopologyTestDriver(filterTopology.topologie(), props)

  @Before
  def setUpAll(): Unit = {
    testDriver
  }


  @Test
  @Order(1)
  @DisplayName("Test 1 : vérification du chiffre d'affaire de la facture")
  def testCAFacture(): Unit = {
    val facture1 = Facture("a326", "téléviseur LG 3A Nano", 3, 3350.75,
      OrderLine("34e", "45i", "20/09/2010", "20/09/2010", 15.00, 700, 10))
    val factureFactory : ConsumerRecordFactory[String, Facture] = new ConsumerRecordFactory("factureBinJSO", stringSer, factureSer)
    testDriver.pipeInput(factureFactory.create("factureBinJSO", facture1.factureid, facture1))

    // lecture du résultat
    val resultatTopology = testDriver.readOutput("topic-test", new StringDeserializer, new DoubleDeserializer)

    // test
    assertEquals("le chiffre d'affaire de la facture doit être égal à : ", 150, resultatTopology.value().toInt)

  }

  @Test
  @Order(2)
  @DisplayName("Test 2 : vérification du chiffre d'affaire de la facture supérieure")
  def testCAFacture2(): Unit = {
    val facture1 = Facture("a326", "téléviseur LG 3A Nano", 3, 3350.75,
      OrderLine("34e", "45i", "20/09/2010", "20/09/2010", 15.00, 700, 10))
    val factureFactory : ConsumerRecordFactory[String, Facture] = new ConsumerRecordFactory("factureBinJSO", stringSer, factureSer)
    testDriver.pipeInput(factureFactory.create("factureBinJSO", facture1.factureid, facture1))

    // lecture du résultat
    val resultatTopology = testDriver.readOutput("topic-test", new StringDeserializer, new DoubleDeserializer)

    // test 2
    assert(150 <= resultatTopology.value().toInt)

  }



  @After
  def cleanUpAll(): Unit = {

    testDriver.close()

  }


}