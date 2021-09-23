package Week11Spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

object Assignment extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val SparkConf = new SparkConf()
  SparkConf.set("spark.app.name","Assignment")
  SparkConf.set("spark.master","local[2]")

  val Spark = SparkSession.builder()
    .config(SparkConf)
    .getOrCreate()

  val windowdataschema = StructType(List(
    StructField("Country", StringType) ,
    StructField("weeknum", IntegerType),
    StructField("numinvoices", IntegerType),
    StructField("totalquantity", IntegerType),
    StructField("invoicevalues", DoubleType)
  ))

 val windowdataDF = Spark.read
   .format("csv")
   .schema(windowdataschema)
   .option("path","src/main/resources/InputFile/Week11Spark/windowdata.csv")
   .load

  windowdataDF.show()

  /*windowdataDF.write
    .partitionBy("Country", "weeknum")
    .mode(SaveMode.Overwrite)
    .option("path","/C:/Users/AnkitaM/Documents/BIGDATA/Week11Spark/windowdata_output").save()  */

  windowdataDF.write.format("avro")
    .partitionBy("Country")
    .mode(SaveMode.Overwrite)
    .option("path","src/main/resources/OutputFile/Week11Spark/windowdata_avrooutput")
    .save()

  Spark.close()



}
