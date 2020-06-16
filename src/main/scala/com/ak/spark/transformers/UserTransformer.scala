package com.ak.spark.transformers

import com.ak.spark.cases.FoodToUserCase
import com.datastax.spark.connector._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object UserTransformer {

  def main(arg: Array[String]): Unit = {

    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "localhost")
    val sparkSession: SparkSession = SparkSession.builder().config(conf).master("local").
      appName("Spark-Scala-Cassandra-UserTransformer").getOrCreate()

    val sc = sparkSession.sparkContext
    val user_rdd = sc.cassandraTable("tutorial", "user")
    print("User table row -> " + user_rdd.first())
    val foodToUserRdd = user_rdd.map(user => new FoodToUserCase(user.getString(1), user.getString(0)))

    //Save to Cassandra
    foodToUserRdd.saveToCassandra("tutorial","food_to_user_index")

  }

}
