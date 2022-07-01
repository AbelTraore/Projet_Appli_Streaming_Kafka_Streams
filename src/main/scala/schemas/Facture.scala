package schemas

import java.util.Date

case class Facture(
                  factureid : String,
                  productName : String,
                  quantite : Integer,
                  total : Double,
                  orderLine: OrderLine
                  )

case class OrderLine(
                      orderlineid : String,
                      productid : String,
                      shipdate : String,
                      billdate : String,
                      unitprice : Double,
                      totalprice : Double,
                      numunits : Int

                    )


case class DetailsCommande(
                            orderid : String,
                            productid : String,
                            shipdate : String,
                            billdate : String,
                            unitprice : Double,
                            numunits : Int,
                            totalprice : Double
                          )

case class Commande(
                     customerid : String,
                     campaignid : String,
                     orderdate : String,
                     city : String,
                     state : String,
                     paymenttype : String
                   )

case class CommandeComplet(
                            orderid : String,
                            productid : String,
                            shipdate : String,
                            billdate : String,
                            unitprice : Double,
                            numunits : Int,
                            totalprice : Double,
                            city : String,
                            state : String
                          )

