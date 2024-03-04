package com.example

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, col, max, min}

object Analysis2 {
  def avgPriceCity(data:DataFrame,city:String):Double= {
    val bikesCity = data.filter(col("city") === city && col("power") > 200)
    val avgPriceDF = bikesCity.agg(avg("price"))
    val averagePrice = avgPriceDF.first().getDouble(0)
    averagePrice
  }
  def maxPrice(data:DataFrame,brand:String):Double=
  {
    val bikebrands = data.filter(col("brand") === brand && col("age") < 4)
    val maxPriceDF = bikebrands.selectExpr("CAST(price AS Double)").agg(max("price"))
    val maxPrice = maxPriceDF.first().getDouble(0)
    maxPrice
  }

 def minPrice(data:DataFrame,bike_name:String):Double={
   val bikeName=data.filter(col("bike_name")===bike_name && col("power")>150)
   val minPriceDF=bikeName.selectExpr("CAST(price AS Double)").agg(min("price"))
   val minPrice=minPriceDF.first().getDouble(0)
   minPrice


 }

  def percentageBikes(data:DataFrame):Double={
    val percentFilter=data.filter(col("price")>20000 && col("age")<6)
    val totalBikeCount=data.count()
    val filteredBikesCount=percentFilter.count()

    val percentage=(filteredBikesCount.toDouble/totalBikeCount.toDouble) * 100.0
    percentage
  }



}
