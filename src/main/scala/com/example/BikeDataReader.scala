package com.example

import org.apache.spark.sql.{DataFrame, SparkSession}

object BikeDataReader {
  def readCSV(filePath: String, spark: SparkSession): DataFrame = {
    spark.read.option("header", "true").csv(filePath)

  }
}