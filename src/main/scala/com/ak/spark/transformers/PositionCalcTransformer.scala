package com.ak.spark.transformers

import com.ak.spark.cases.PositionCalcEntity
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

    //    print("legalEntity table row -> " + legalEntityRdd.first())

    println("legalEntityRdd ->>")
    val legalEntityList = sc.broadcast(legalEntityRdd.collect()).value

    println("accountRddList ->>")
    val accountRddList = sc.broadcast(accountRdd.collect()).value

    println("assetCompositeRddList ->>")
    val assetCompositeRddList = sc.broadcast(assetCompositeRdd.collect()).value

    println("positionRddList ->>")
    val positionRddList = sc.broadcast(positionRdd.collect()).value


    /*val rddList = RDD[PositionCalcEntity]
    val positionList = new util.ArrayList[PositionCalcEntity]()
    for (legalEntity <- legalEntityList) {
      for (accountEntity <- accountRddList) {
        for (assetEntity <- assetCompositeRddList) {
          for (positionEntity <- positionRddList) {
            val pos = generatePositionCalcEntity(legalEntity, accountEntity, assetEntity, positionEntity)
            rddList.
            val t = new FoodToUserCase("","")
            pos.saveToCassandra("tutorial", "food_to_user_index")
          }
        }
      }
    }*/

    /*legalEntityRdd.joinWithCassandraTable("cc_engine", "account").on(SomeColumns("le_id")).joinWithCassandraTable("cc_engine", "asset_composite").joinWithCassandraTable("cc_engine", "position").on(SomeColumns("asset_id","account_id")).foreach(println)*/
    val join1 = legalEntityRdd.joinWithCassandraTable("cc_engine", "account").on(SomeColumns("le_id"))
    join1.foreach(println)
    println("Check---1")
    val join2 = accountRdd.joinWithCassandraTable("cc_engine", "position").on(SomeColumns("account_id"))
    join2.foreach(println)
    println("Check---2")
//    val join3 = assetCompositeRdd.joinWithCassandraTable("cc_engine", "position").on(SomeColumns("asset_id")).joinWithCassandraTable("cc_engine", "account").on(SomeColumns("account_id"))

    val join3 = positionRdd.joinWithCassandraTable("cc_engine", "asset_composite").on(SomeColumns("asset_id"))

//    join1.as("d1").join(join2.as("d2"), join1.col())
    val d1 = join1.join(join2,Set("account_id"))

    join4.foreach(println)
    println("Check---3")

//    val join4 = join2.join(join3).foreach(println)
    /*val join2 = join1.joinWithCassandraTable("cc_engine", "asset_composite").joinWithCassandraTable("cc_engine", "position").on(SomeColumns("asset_id","account_id")).foreach(println)*/

    println("ENDDDD---")

  }

  def generatePositionCalcEntity(legalEntity: CassandraRow, accountEntity: CassandraRow, assetEntity: CassandraRow, positionEntity: CassandraRow): PositionCalcEntity = {

    /*val marketValue = assetEntity.getDecimal("quantity") * assetEntity.getDecimal("price")
    print("marketval " + marketValue)
*/
    return PositionCalcEntity(accountEntity.getString("account_id"), accountEntity.getString("account_name"), accountEntity.getString("account_type"), legalEntity.getString("le_id"), legalEntity.getString("le_name"), legalEntity.getString("le_type"), assetEntity.getString("asset_id"), assetEntity.getString("asset_name"), assetEntity.getString("country_code"), assetEntity.getString("currency_code"), assetEntity.getString("market_code"), assetEntity.getDecimal("price"), positionEntity.getDecimal("quantity"), 0)
  }

}
