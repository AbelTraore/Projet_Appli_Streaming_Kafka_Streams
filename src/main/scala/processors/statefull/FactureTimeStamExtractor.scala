package processors.statefull

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.processor.TimestampExtractor
import schemas.Facture

import java.util.Date


class FactureTimeStamExtractor extends TimestampExtractor{

  override def extract(record: ConsumerRecord[AnyRef, AnyRef], previousTimeStamp: Long): Long = {

    return  record.value() match {
      case r : Facture => r.orderLine.billdate.asInstanceOf[Date].toInstant.toEpochMilli
      case _  => throw new RuntimeException(s" erreur dans le parsing. Les messages ne sont pas des instances de facture : ${record.value()}")
    }


  }
}
