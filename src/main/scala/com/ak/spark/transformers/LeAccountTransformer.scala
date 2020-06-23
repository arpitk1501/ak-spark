package com.ak.spark.transformers

import com.ak.spark.cases.LeAccountEntity
import com.datastax.spark.connector.SomeColumns
import org.apache.spark.SparkConf
import com.datastax.spark.connector._
import org.apache.spark.sql.SparkSession

object LeAccountTransformer {

  def main(arg: Array[String]): Unit = {

    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "localhost")
    val sparkSession: SparkSession = SparkSession.builder().config(conf).
      master("local").appName("Spark Scala-Cassandra-CC-LEAccount").getOrCreate()

    val sc = sparkSession.sparkContext

    val leAccountRdd = sc.cassandraTable("cc_engine", "legal_entity").joinWithCassandraTable("cc_engine", "account").
      on(SomeColumns("le_id"))
    leAccountRdd.toDebugString
    leAccountRdd.collect().foreach(println)

    val leAccountEntityRdd = leAccountRdd.map(row => {
      LeAccountEntity(row._1.getString("le_id"), row._1.getString("le_name"),
        row._1.getString("le_type"), row._2.getString("account_id"),
        row._2.getString("account_name"), row._2.getString("account_type"))
    })
    println("before leAccountEntityRdd")
    leAccountEntityRdd.collect().foreach(println)

    leAccountEntityRdd.saveToCassandra("cc_engine", "le_account")

  }
}
