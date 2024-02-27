package com.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object WordCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    // Create SparkSession
    val spark = SparkSession.builder()
      .appName("WordFrequencyCSV")
      .master("local[*]")
      .getOrCreate()

    // Read CSV file into DataFrame
    val csvFile = "data/business.csv" // Path to your CSV file
    val df = spark.read.option("header", "true").csv(csvFile)

    // Tokenize text and count word frequencies
    val wordFrequency = df
      .select(explode(split(lower(col("industry")), "\\s+")).as("word"))

    //Count the number frequencies
    val numberFrequency=df
      .select(explode(split(lower(col("value")),"\\s+")).as("word"))

    //Combining both columns to get total count
    val totalWords=wordFrequency.union(numberFrequency)
        .groupBy("word")
        .count()
        .orderBy(desc("count"))
        .repartition(1)

    // Display word frequencies
    println("Word frequencies:")
    totalWords.show(truncate = false)

    //Writing output to a csv file
    totalWords.write.option("header","true").csv("/home/mileenamartin/Documents/WordCount")

    // Stop SparkSession
    spark.stop()
  }

}
