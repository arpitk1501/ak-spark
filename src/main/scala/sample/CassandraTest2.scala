package sample

import com.datastax.spark.connector._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CassandraTest2 {
  def main(arg: Array[String]): Unit = {

    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "localhost")
    val sparkSession: SparkSession = SparkSession.builder().config(conf).master("local").appName("Spark Scala-Cassandra-2").getOrCreate()

    val sc = sparkSession.sparkContext
    val emp_rdd = sc.cassandraTable("ak_test", "emp")
    print("EMP table row -> " + emp_rdd.first)


  }

}
