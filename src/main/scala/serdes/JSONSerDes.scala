package serdes

import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import schemas.Facture

import java.util

/*
class JSONSerDes[T] extends Serde[T] {

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}

  override def serializer(): Serializer[T] = new JSONSerializer[T]

  override def deserializer(): Deserializer[Facture] = new JSONDeserializer

}

*/

