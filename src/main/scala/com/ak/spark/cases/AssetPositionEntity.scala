package com.ak.spark.cases

import scala.math.BigDecimal

case class AssetPositionEntity(accountId: String, assetId: String, quantity: BigDecimal,
                               assetName: String, countryCode: String, currencyCode: String,
                               marketCode: String, price: BigDecimal)
