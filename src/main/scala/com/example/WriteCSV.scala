package com.example

import org.apache.spark.sql.SparkSession

object WriteCSV {
  def main(args:Array[String]):Unit={
    val spark=SparkSession.builder()
      .appName("Write to CSV")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val data=Seq(
      (1,"Alice","20-09-2002"),
      (2,"BOb","30-08-1998"),
      (3,"Charlie","10-09-2002"),
      (5,"Dan","07-03-1999") //Writing data
    )toDF("id","name","dob")


    data.write.option("header",value=true).mode(saveMode="overwrite").csv("/home/mileenamartin/Documents/WriteCSV") // overwrite is used to update an already written file
    val dataDF=spark.read.option("header",value=true).csv("/home/mileenamartin/Documents/WriteCSV")
    dataDF.show()

    spark.stop()



  }

}
