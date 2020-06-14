package sample

import org.apache.spark.sql.SparkSession

object indianpincodeCsv {

  def main(arg: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().master("local").appName("Spark Scala").getOrCreate()
    val csvPO = sparkSession.read.option("header", true).csv("Pincode_data.csv")

    csvPO.createOrReplaceTempView("csvView")
    val count: Long = sparkSession.sql("select * from csvView").count()
    println("Total Records in File -> "+ count)
  }
}
