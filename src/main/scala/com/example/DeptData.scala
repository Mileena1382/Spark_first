package com.example

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.reflect.io.File

object DeptData {
  def filterAndWriteToSeparateFiles(df: DataFrame, spark: SparkSession,outputPath:String): Unit = {
    // Get unique userOrgNames
    val userOrgNames = df.select("userOrgName").distinct().collect()

    // Filter and write to separate files for each userOrgName
    userOrgNames.foreach { row =>
      val userOrgName = row.getString(0)
      val filteredDF = df.filter(s"userOrgName = '$userOrgName'")
//      val outputPath = "/home/mileenamartin/Documents/"
      filteredDF.write.option("header","true").csv(s"$outputPath/$userOrgName")


    }
  }


}
