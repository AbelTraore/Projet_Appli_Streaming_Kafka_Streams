package processors.statefull


import org.apache.kafka.streams.scala._
import java.util.Properties
import org.apache.kafka.streams.{StreamsConfig, Topology}
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.KafkaStreams
import java.time.Duration



object CountProcessor {


  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.Serdes.String
  import org.apache.kafka.streams.scala.Serdes._

  def main(args: Array[String]): Unit = {

    val str : StreamsBuilder = new StreamsBuilder()
    val kstr : KStream[String, String] = str.stream[String, String]("streams_app")
    val kstrMaj : KStream[String, String] = kstr.mapValues(v => v.toUpperCase)
    val kcount : KTable[String, Long] = kstrMaj.flatMapValues(r => r.split(","))
      .groupBy((_, valeur) => valeur)
      .count()(Materialized.as("counts-store"))       // state store
    kcount.toStream.to("streams_count")

    val topologie : Topology = str.build()
    val kkStream : KafkaStreams = new KafkaStreams(topologie, getParams("localhost:9092"))
    kkStream.start()

    // visualiser la topologie
    println(topologie.describe())

    sys.ShutdownHookThread {
      kkStream.close(Duration.ofSeconds(10))
    }

  }

  def getParams(bootStrapServer : String) : Properties = {

    val props : Properties = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "helloWorld")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer)
    props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)
    props.put("auto.offset.reset.config", "latest")
    //props.put("consumer.group.id", "hello world")


    props

  }


}

