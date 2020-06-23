package com.ak.spark.transformers

import com.ak.spark.cases.UserAddressEntity
import com.datastax.spark.connector._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object UserAddressTransformer {

  def main(arg: Array[String]): Unit = {

    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "localhost")
    val sparkSession: SparkSession = SparkSession.builder().config(conf).master("local").
      appName("Spark-Scala-Cassandra-UserAddressTransformer").getOrCreate()
    val sc = sparkSession.sparkContext
    val userRdd = sc.cassandraTable("tutorial", "user")
    val addressRdd = sc.cassandraTable("tutorial", "address")
    println("User table row -> " + userRdd.first())
    println("Address table row -> " + addressRdd.first())
    //    val foodToUserRdd = user_rdd.map(user => new UserAddressEntity(user.getString(1), user.getString(0)))

    val joinRdd = userRdd.joinWithCassandraTable("tutorial", "address")
    joinRdd.toDebugString
    joinRdd.collect().foreach(println)

    val userAddressEntityRdd = joinRdd.map(row => {
      UserAddressEntity(row._1.getString("name"), row._1.getString("favorite_food"),
        row._2.getString("address_line") + row._2.getString("city"),
        row._1.getString("name") + row._2.getString("city"))

    })

    //Save to Cassandra
    println("saving to Table => " + userAddressEntityRdd)
//    userAddressEntityRdd.saveToCassandra("tutorial", "user_address")

  }

}
