package com.example

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SparkSession, functions}
import java.time.{Duration, Instant}


object BigFile {
  def main(args:Array[String]):Unit={
    val spark=SparkSession.builder()
      .appName("Big Data File Processing")
      .master("local[*]")
      .getOrCreate()

    val filePath= "/home/mileenamartin/Documents/bigdata.txt"
    val appendedFilePath= "/home/mileenamartin/Documents/bigdataappended.txt"

    val initialText=generateText(10)
    writeTextToFile(spark,filePath,initialText)

    val moreText=generateText(5)
    appendTextToFile(spark,appendedFilePath,moreText,filePath)

    val startTime=Instant.now()
    val textDF=spark.read.text(filePath)
    val processedDF=textDF.withColumn("length",functions.length(col("value")))
    processedDF.show(truncate = false)
    val endTime=Instant.now()
    val durationSeconds=Duration.between(startTime,endTime).toMillis/1000.0
    println(s"Time taken to read and process the file: $durationSeconds seconds")
    processedDF.write.text(appendedFilePath)
    spark.stop()
  }
  def writeTextToFile(spark: SparkSession, filePath: String, text: String):Unit= {
    import spark.implicits._
    Seq(text).toDF("value").write.mode("overwrite").text(filePath)
  }

  def appendTextToFile(spark: SparkSession, afilePath: String, text: String, filePath: String): Unit = {
    import spark.implicits._
    // Read the content of the original file
    val existingDF = spark.read.text(filePath)
    // Append the text to the original content
    val appendedContent = existingDF.as[String].collect().mkString("\n") + "\n" + text
    // Write the combined content back to the original file
    writeTextToFile(spark, afilePath, appendedContent)
  }
  def generateText(i: Int):String={
    val text=(1 to i).map (i=> s"Line $i: This is some random data.").mkString("\n")
    text
  }
}

