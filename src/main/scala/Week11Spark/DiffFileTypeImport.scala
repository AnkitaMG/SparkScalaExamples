package Week11Spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DiffFileTypeImport extends App{

  val SparkConf = new SparkConf()
  SparkConf .set("spark.app.name","file load")
  SparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(SparkConf)
    .getOrCreate()

  /*val ordersDf = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema",true)
    .option("path","src/main/resources/InputFile/Week11Spark/orders.csv").load */

    /*val ordersDF =spark.read
      .format("json")
      .option("mode","FAILFAST")
      .option("path","src/main/resources/InputFile/Week11Spark/players.json").load */

  val ordersDF = spark.read
   // .format("parquet")
    .option("path","src/main/resources/InputFile/Week11Spark/users.parquet").load

  ordersDF.printSchema
  ordersDF.show(false)

 // scala.io.StdIn.readLine()
  spark.stop()

}
