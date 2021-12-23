import schemas.{Facture, OrderLine}

object TestSerDes {

  def main(args: Array[String]): Unit = {
    val facture1 = Facture("a320", "téléviseur LG 3A Nano", 3, 3350.75, OrderLine("34e", "45i", "20/09/2010", "20/09/2010", 15.00, 700, 10))

    println(facture1.orderLine.totalprice)


  }

}
