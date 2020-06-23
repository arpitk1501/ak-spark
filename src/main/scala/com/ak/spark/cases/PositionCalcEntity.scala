package com.ak.spark.cases

import scala.math.BigDecimal

case class PositionCalcEntity(accountId: String, accountName: String,
                              accountType: String, leId: String, leName: String,
                              leType: String, assetId: String, assetName: String,
                              countryCode: String, currencyCode: String, marketCode: String,
                              price: BigDecimal, quantity: BigDecimal, marketValue: BigDecimal)


