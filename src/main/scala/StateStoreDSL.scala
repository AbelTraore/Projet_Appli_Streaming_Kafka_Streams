import KTableComputations.str
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.scala._

import java.util.Properties
import org.apache.kafka.streams.{StreamsConfig, Topology}
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.kstream.Printed
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder
import org.apache.kafka.streams.state.{KeyValueBytesStoreSupplier, KeyValueStore, StoreBuilder, Stores}

import java.time.Duration


object StateStoreDSL extends App {


  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.Serdes._


  val props: Properties = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "state-store-dsl")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")


  val props_state : java.util.Map[String, String] = new java.util.HashMap[String, String]()
  props_state.put("retention.ms","172800000" )
  props_state.put("retention.bytes", "10000000000")
  props_state.put("cleanup.policy", "compact")
  props_state.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "10000")
  props_state.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "10485760")



  val factureStoreName = "factureStore"
  val factureStoreSupplier : KeyValueBytesStoreSupplier = Stores.persistentKeyValueStore(factureStoreName)
  val factureStoreBuilder : StoreBuilder[KeyValueStore[String, String]] = Stores.keyValueStoreBuilder(factureStoreSupplier, String, String)


  val str : StreamsBuilder = new StreamsBuilder()
  str.addStateStore(factureStoreBuilder)


  val ktblTest: KTable[String, String] = str.table("ktabletest", Materialized.as(factureStoreSupplier)(String, String)
    .withCachingEnabled()
  .withLoggingEnabled(props_state))


  ktblTest.toStream.print(Printed.toSysOut().withLabel("Clé/Valeur du KTable"))


  val topologie: Topology = str.build()
  val kkStream: KafkaStreams = new KafkaStreams(topologie, props)
  kkStream.start()

  sys.ShutdownHookThread {
    kkStream.close()
  }


}
