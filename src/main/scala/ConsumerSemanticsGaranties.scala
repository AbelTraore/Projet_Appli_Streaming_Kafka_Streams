import java.time.Duration
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.protocol
import org.apache.kafka.common.serialization._
import org.apache.kafka.common._
import org.apache.kafka.common.security.auth.SecurityProtocol

import java.util.Properties
import java.util.Collections
import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.collection.JavaConversions._





object ConsumerSemanticsGaranties {

  def main(args: Array[String]): Unit = {


    val consumer = new KafkaConsumer[String, String](getKafkaConsumerParams("localhost:9092", "groupe_orders"))
    val producer = new KafkaProducer[String, String](getKafkaProducerParams_prod("localhost:9092"))
    val liste_offsets : Map[TopicPartition, OffsetAndMetadata] = Map()

    producer.initTransactions()

    try {

      consumer.subscribe(Collections.singletonList("orderline")) //on pouvait aussi faire ceci : List(topic_list).asJava

      while (true) {
        val messages: ConsumerRecords[String, String] = consumer.poll(Duration.ofSeconds(30))

        producer.beginTransaction()

        if (!messages.isEmpty) {
          println("Nombre de messages collectés dans la fenêtre :" + messages.count())
          for (message <- messages.asScala) {
            println("Topic: " + message.topic() +
              ",Key: " + message.key() +
              ",Value: " + message.value() +
              ", Offset: " + message.offset() +
              ", Partition: " + message.partition())
            producer.send(new ProducerRecord[String, String]("topic_transaction", message.value())) // il faudra créer le topic transaction
          }

          for(partition <- messages.partitions()) {
            val listPartitionMessages = messages.records(partition)
            val offset = listPartitionMessages.get(listPartitionMessages.size() - 1).offset()
            liste_offsets.add(partition, new OffsetAndMetadata(offset + 1))

          }
        }

        producer.sendOffsetsToTransaction(liste_offsets, "groupe_orders")

      }
      consumer.commitAsync() // ou bien consumer.commitSync()
      }  catch {
    case ex: CommitFailedException =>
      println("erreur dans le commit des offset. Kafka n'a pas reçu le jeton de reconnaissance confirmant que nous avons bien reçu les données")
    case ex : Exception =>
      println("erreur dans le consumer")
       }

    }

    def getKafkaConsumerParams (kafkaBootStrapServers : String, KafkaConsumerGroupId : String) : Properties = {

      val props: Properties = new Properties()
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootStrapServers)
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
      props.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaConsumerGroupId)
      props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
      props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "roundrobin")
      props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG,"read_committed")

      return props

    }

  def getKafkaProducerParams_prod (KafkaBootStrapServers : String) : Properties = {

    val props : Properties = new Properties()
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaBootStrapServers)
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transaction_1")

    return props

  }

  }

