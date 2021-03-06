package processors.statefull

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.processor.TimestampExtractor
import schemas.Facture

import java.util.Date
import java.text.SimpleDateFormat


class FactureTimeStamExtractor extends TimestampExtractor{

  override def extract(record: ConsumerRecord[AnyRef, AnyRef], previousTimeStamp: Long): Long = {

    return  record.value() match {
      case r : Facture => {
        //r.orderLine.billdate.asInstanceOf[Date].toInstant.toEpochMilli
        val billDate = r.orderLine.billdate
        val formattedBillDate = new SimpleDateFormat("dd/MM/yyyy")
        val transformedBillDate = formattedBillDate.parse(billDate).toInstant.toEpochMilli
        transformedBillDate
      }
      case _  => throw new RuntimeException(s" erreur dans le parsing. Les messages ne sont pas des instances de facture : ${record.value()}")
    }


  }
}
