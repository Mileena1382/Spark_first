package com.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object DataMainFile {
  def main(args:Array[String]):Unit={
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark=SparkSession.builder()
      .appName("Data analysis")
      .master("local[*]")
      .getOrCreate()

//    val csvDF=spark.read.option("header","true").csv("data/UserData.csv")
    val filePath = "data/UserData.csv"
    val csvDF = Reader.readCSV(filePath, spark)

    val outputPath = "/home/mileenamartin/Documents/DeptData"
    DeptData.filterAndWriteToSeparateFiles(csvDF,spark,outputPath )

  }


}