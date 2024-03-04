package com.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object CSVanalysis {
  def main(args:Array[String]):Unit= {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark=SparkSession.builder()
      .appName("Data analysis")
      .master("local[*]")
      .getOrCreate()

    val filePath="data/Used_Bikes.csv"
    val csvDF=BikeDataReader.readCSV(filePath, spark)

    val yamahaBikeCount=Analysis1.yamahaCounts(csvDF)
    println(s"Number of Yamaha bikes with power more than 150 cc and first owner: $yamahaBikeCount")


    val RTRCount=Analysis1.RTRrows(csvDF)
    println(s"Number of RTR=$RTRCount")

    val bikesMumbai=Analysis1.bikesInMumbai(csvDF)
    bikesMumbai.show()

    var city=" "
    println("Enter the city whose average price of bikes is to be calculated:")
    city=scala.io.StdIn.readLine()
    val avgPrice=Analysis2.avgPriceCity(csvDF,city) // If user is inputing the city name
//    val avgPrice=Analysis2.avgPriceCity(csvDF,"Delhi") // if passing variable name in code itself
    println(s"Average price of bikes in specified city is $avgPrice")


    var brand=""
    print("Enter the brand with age less than 4 to find max price:")
    brand=scala.io.StdIn.readLine()
    val maxPriceBike=Analysis2.maxPrice(csvDF,brand)
    println(s"Max price of bike of specified bike is $maxPriceBike")


    var bike_name=""
    println("Enter the bike name with power greater than 150 to find min price")
    bike_name=scala.io.StdIn.readLine()
    val minPriceBike=Analysis2.minPrice(csvDF,bike_name)
    println(s"Min price of specified bike is $minPriceBike")

    val percentageOfBikes=Analysis2.percentageBikes(csvDF)
    println(s"Percenatge of bikes above Rs 50000 is : $percentageOfBikes %")

    spark.stop()
  }


}
