package Week11Spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSessionExample extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "my first application")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  val ordersDf = spark.read
    .option("header", true)
   // .option("inferSchema",true)
    .csv("src/main/resources/InputFile/Week11Spark/orders.csv")

  //ordersDf.filter("order_ids  < 10").show()

  val calDf = ordersDf
    .repartition(4)
    .where("order_customer_id >1000")
    .select("order_id","order_customer_id")
    .groupBy("order_customer_id")
    .count()

  calDf.show()
  calDf.printSchema()

  Logger.getLogger(getClass.getName).info("My application is completed successfully")

  spark.stop()

}
