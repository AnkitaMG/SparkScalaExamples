package Week11Spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SchemaExample extends App{

 // case class Orders(order_id: Int, order_date: Timestamp, customer_id: Int, order_status: Int)

  Logger.getLogger("org").setLevel(Level.ERROR)

  val SparkConf = new SparkConf()
  SparkConf.set("spark.app.name","Set schema Manually")
  SparkConf.set("spark.master", "local[2]")

  val Spark = SparkSession.builder()
    .config(SparkConf)
    .getOrCreate()

  /*val ordersSchema = StructType(List(
    StructField("orderid", IntegerType),
    StructField("orderdate", StringType),
    StructField("customerid", IntegerType),
    StructField("status", StringType)
  )) */

  val ordersSchemaDDL = "orderid Int, orderdate String, customerid Int, status String"

  val ordersDf =Spark.read
  .format("csv")
    .option("header", true)
    .option("inferSchema",true)
    .schema(ordersSchemaDDL)
    .option("path","src/main/resources/InputFile/Week11Spark/orders.csv").load

  //import Spark.implicits._
  //val orderDs = ordersDf.as[Orders]
 // orderDs.filter(x => x.customer_id)


  ordersDf.printSchema()
  ordersDf.show()
Spark.stop()

}
