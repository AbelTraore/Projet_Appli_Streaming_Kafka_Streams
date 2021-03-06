package serdes

import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}


import java.util


class BytesSerdes [T]  extends Serde[T] {

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}

  override def serializer(): Serializer[T] = new BytesSerializer[T]

  override def deserializer(): Deserializer[T] = new BytesDeserializer[T]


}
