package sample.configs

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CassandraTest1 {
  def main(arg: Array[String]): Unit = {
    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "localhost")
    val sparkSession: SparkSession = SparkSession.builder().config(conf).master("local").appName("Spark Scala-Cassandra").getOrCreate()

    val sc = sparkSession.sparkContext
    print(sc.parallelize(1 to 50).sum())

  }

}
