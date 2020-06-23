package com.ak.spark.transformers

import com.ak.spark.cases.{AccountPositionEntity, AssetPositionEntity, LeAccountEntity, PositionCalcEntity}
import com.datastax.spark.connector._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object PositionCalcTransformer {

  def main(arg: Array[String]): Unit = {

    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "localhost")
    val sparkSession: SparkSession = SparkSession.builder().config(conf).master("local").appName("Spark Scala-Cassandra-CC").getOrCreate()

    val sc = sparkSession.sparkContext
    val legalEntityRdd = sc.cassandraTable("cc_engine", "legal_entity")
    val accountRdd = sc.cassandraTable("cc_engine", "account")
    val assetCompositeRdd = sc.cassandraTable("cc_engine", "asset_composite")
    val positionRdd = sc.cassandraTable("cc_engine", "position")

    val leAccountRdd = sc.cassandraTable("cc_engine", "legal_entity").joinWithCassandraTable("cc_engine", "account").
      on(SomeColumns("le_id"))
    leAccountRdd.toDebugString
    leAccountRdd.collect().foreach(println)

    val leAccountEntityRdd = leAccountRdd.map(row => {
      LeAccountEntity(row._2.getString("account_id"), row._2.getString("account_name"),
        row._2.getString("account_type"), row._1.getString("le_id"),
        row._1.getString("le_name"), row._1.getString("le_type"))
    })
    leAccountRdd.toDebugString
    println("before LeAccountEntity")
    leAccountEntityRdd.collect().foreach(println)

    val accountPositionRdd = sc.cassandraTable("cc_engine", "account").joinWithCassandraTable("cc_engine", "position").
      on(SomeColumns("account_id"))
    accountPositionRdd.toDebugString
    accountPositionRdd.collect().foreach(println)

    val accountPositionEntityRdd = accountPositionRdd.map(row => {
      AccountPositionEntity(row._1.getString("account_id"), row._1.getString("account_name"),
        row._1.getString("account_type"), row._2.getString("asset_id"),
        row._2.getDecimal("quantity"))
    })
    accountPositionEntityRdd.toDebugString
    println("before accountPositionEntityRdd")
    accountPositionEntityRdd.collect().foreach(println)

    val assetPositionRdd = sc.cassandraTable("cc_engine", "position").joinWithCassandraTable("cc_engine", "asset_composite").
      on(SomeColumns("asset_id"))
    assetPositionRdd.toDebugString
    assetPositionRdd.collect().foreach(println)

    val assetPositionRddEntityRdd = assetPositionRdd.map(row => {
      AssetPositionEntity(row._1.getString("account_id"), row._1.getString("asset_id"),
        row._1.getDecimal("quantity"), row._2.getString("asset_name"), row._2.getString("country_code"), row._2.getString("currency_code"), row._2.getString("market_code"), row._2.getDecimal("price"))
    })
    assetPositionRddEntityRdd.toDebugString
    println("before assetPositionRddEntityRdd")
    assetPositionRddEntityRdd.collect().foreach(println)

    println("Final RDD-")
    val joinRdd = leAccountRdd.fullOuterJoin(assetPositionRdd)

    //    val joinRdd = sc.cassandraTable("cc_engine", "legal_entity").joinWithCassandraTable("cc_engine", "account").
    //      on(SomeColumns("le_id")).joinWithCassandraTable("cc_engine", "position").on(SomeColumns("account_id"))
    //      .joinWithCassandraTable("cc_engine", "legal_entity").on(SomeColumns("le_id"))
    joinRdd.toDebugString
    joinRdd.collect().foreach(println)
//        joinRdd.foreach(println)

    println("count -")
    println(joinRdd.count)

    joinRdd.collect().map(row => {
      println("row -> ")
      println(row)
      println("row._1 -> ")
      println(row._1)
      println("row._2 -> ")
      println(row._2)
      println("row._2._1 -> ")
      println(row._2._1)
      println("row._2._2 -> ")
      println(row._2._2)
    })

    println("End-->")
    /*val positionCalcRddEntityRdd = joinRdd.map(row => {
      PositionCalcEntity(row._1.getString("account_id"),
        row._1.getString("account_name"),
        row._1.getString("account_type"),
        row._1.getString("le_id"),
        row._1.getString("le_name"),
        row._1.getString("le_type"),
        row._2._2.get.getString("asset_id"),
        row._2._2.get.getString("asset_name"),
        row._2._2.get.getString("country_code"),
        row._2._2.get.getString("currency_code"),
        row._2._2.get.getString("market_code"),
        row._2._2.get.getDecimal("price"),
        row._1.getDecimal("quantity"),
        BigDecimal.apply("1234"))
    })


    positionCalcRddEntityRdd.toDebugString
    println("before positionCalcRddEntityRdd")
    positionCalcRddEntityRdd.collect().foreach(println)
    println("before positionCalcRddEntityRdd - 2")
    positionCalcRddEntityRdd.foreach(println)
*/
  }


}
