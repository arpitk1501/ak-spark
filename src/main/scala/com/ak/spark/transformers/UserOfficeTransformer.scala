package com.ak.spark.transformers

import com.ak.spark.cases.UserOfficeDetailsEntity
import com.datastax.spark.connector._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object UserOfficeTransformer {

  def main(arg: Array[String]): Unit = {

    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "localhost")
    val sparkSession: SparkSession = SparkSession.builder().config(conf).master("local").
      appName("Spark-Scala-Cassandra-UserOfficeTransformer").getOrCreate()

    val sc = sparkSession.sparkContext
    val userRdd = sc.cassandraTable("tutorial", "user")
    val officeRdd = sc.cassandraTable("tutorial", "office")
    println("User table row -> " + userRdd.first())
    println("Office table row -> " + officeRdd.first())

    val joinRdd = userRdd.leftJoinWithCassandraTable("tutorial", "office").on(SomeColumns("office_name"))

    joinRdd.toDebugString
    joinRdd.collect().foreach(println)
//
//    val userOfficeEntityRdd = joinRdd.map(row => {
//      UserOfficeDetailsEntity(row._1.getString("name"), row._2.getString("favorite_food"),
//        "ASSOCIATE", BigDecimal.apply(111000.00))
//
//    })
    //Save to Cassandra
//    println("saving to Table user_office_details => " + userOfficeEntityRdd)

    //    userOfficeEntityRdd.saveToCassandra("tutorial", "user_office_details")


  }
}
