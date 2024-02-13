package com.example


import org.apache.spark.sql.{SparkSession,DataFrame}



object WriteFiles {
  def main(args:Array[String]):Unit={
    val spark=SparkSession.builder()
      .appName("Write_text")
      .master("local[*]")
      .getOrCreate()


    // Generate a large DataFrame with dummy data
    val numOfFiles = 2
    val df = generateTextDataFrame(spark, numOfFiles)

    // Write the DataFrame to a file
    df.write.text("/home/mileenamartin/Documents/WriteExample") // Specify the output directory where the file will be saved

    // Stop the SparkSession
    spark.stop()
  }
  def generateTextDataFrame(spark: SparkSession, numFiles: Int):DataFrame={
    import spark.implicits._
    val data=(1 to numFiles).map{ i=>
      s"Line $i: This is some text data."
    }

    val df=data.toDF("Text")
    df
  }


}
