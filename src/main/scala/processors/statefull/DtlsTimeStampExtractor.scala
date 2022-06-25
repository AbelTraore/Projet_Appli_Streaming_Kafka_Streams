package processors.statefull

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.processor.TimestampExtractor
import schemas.DetailsCommande

import java.util.Date
import java.text.SimpleDateFormat
import java.time.Instant




class DtlsTimeStampExtractor extends TimestampExtractor {


  override def extract(record: ConsumerRecord[AnyRef, AnyRef], previousTimeStamp: Long): Long = {

    return  record.value() match {
      case r : DetailsCommande => {
        val billDate = Instant.parse(r.billdate).toEpochMilli
        billDate
      }
      case _  => throw new RuntimeException(s" erreur dans le parsing. Les messages ne sont pas des instances de détails de commande : ${record.value()}")
    }


  }
}
