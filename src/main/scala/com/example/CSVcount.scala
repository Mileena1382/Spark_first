package com.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object CSVcount {

  def main(args:Array[String]):Unit={
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark=SparkSession.builder()
      .appName("CSV count")
      .master("local[*]")
      .getOrCreate()

    // Read the CSV file into a DataFrame
    val df = spark.read.option("header", "true").csv("data/business.csv")

    // Specify the word to count its frequency
    val wordToCount = "57"

    // Filter the DataFrame for the specified word
    val wordFrequency = df
      .selectExpr("explode(split(lower(value), ' ')) as word")
      .filter(col("word") === wordToCount)
      .count()

    // Display the frequency of the specified word
//    val count = wordFrequency.collect()(2).getLong(0)
    println(s"The frequency of '$wordToCount' is: $wordFrequency")

    // Stop the SparkSession
    spark.stop()
  }

}
