package com.ak.spark.transformers

import com.ak.spark.cases.PositionCalcEntity
import com.datastax.spark.connector._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object PositionCalcTransformer_v2 {

  def main(arg: Array[String]): Unit = {

    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "localhost")
    val sparkSession: SparkSession = SparkSession.builder().config(conf).master("local").
      appName("Spark Scala-Cassandra-CC-v2").getOrCreate()

    val sc = sparkSession.sparkContext
    val sc_sql = sparkSession.sqlContext

    //DF
    val leAcc = sc_sql.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "le_account", "keyspace" -> "cc_engine")).load()
    val position = sc_sql.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "position", "keyspace" -> "cc_engine")).load()
    val asset = sc_sql.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "asset_composite", "keyspace" -> "cc_engine")).load()

    val join = leAcc.join(position, leAcc("account_id") === position("account_id"), "inner")
      .join(asset, position("asset_id") === asset("asset_id"), "inner")

    println("join 4-> ")
    join.show()

    val joinRdd = join.rdd.map(row => {
      PositionCalcEntity (row.getString(0), row.getString(1), row.getString(2), row.getString(3),
        row.getString(4),row.getString(5),row.getString(7),row.getString(10),row.getString(11),row.getString(12),
        row.getString(13),row.getDecimal(14),
        row.getDecimal(8), row.getDecimal(8).multiply(row.getDecimal(14)))
    })

    joinRdd.toDebugString
    println("before joinRdd")
    joinRdd.collect().foreach(println)

    joinRdd.saveToCassandra("cc_engine", "position_calc")

  }
}
