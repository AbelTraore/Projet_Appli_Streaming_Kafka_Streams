package serdes

import org.apache.kafka.common.serialization.Serializer
import java.util




class GenericSerializer[T] extends Serializer[T] {

  private val arrBytes = Array.emptyByteArray

  override def configure(configs : util.Map[String, _], isKey: Boolean) : Unit = {}

  override def serialize(s: String, data: T): Array[Byte] = {
    if (data == null) {
      return null
    }else {
      try {
        //c'est ici qu'on procède à la sérialisation
        arrBytes
      } catch {
        // gestionnaire d'erreur de la bibliothèque de Serdes que j'ai choisi
        case e : Exception => throw new Exception(s"Erreur dans la sérialisation de  + ${data.getClass.getName}")

      }
    }
  }

  override def close(): Unit = {}
}
