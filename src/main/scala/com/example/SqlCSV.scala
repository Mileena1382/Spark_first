package com.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, date_format, from_unixtime, lit, when}

object SqlCSV {
  def main(args:Array[String]):Unit={
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark=SparkSession.builder()
      .appName("Csv and sql")
      .master("local[*]")
      .getOrCreate()


    //Reading csv file
    val csvDF=spark.read.option("header","true").csv("data/UserCourseData.csv")
//    csvDF.show()
//    csvDF.printSchema()


    //Reading sql table
    val sqlDF=spark.read
      .format("jdbc")
      .option("url","jdbc:mysql://localhost:3306/task_one")
      .option("dbtable","user")
      .option("user","root")
      .option("password","Welcome@123")
      .load()
//    sqlDF.show(),
//    sqlDF.printSchema()

   //Joining both dataframes
    val joinedDF=csvDF.join(sqlDF,"userID")
//    joinedDF.show()
//    joinedDF.printSchema()

    // Convert completedOn column from Unix timestamp to desired format
    val timeDF = joinedDF.withColumn("completedOn",
      date_format(from_unixtime(col("completedOn")), "yyyy/MM/dd hh:mm:ss a")
    )
//    timeDF.show()

    // Convert status from digits to text
    val statusDF=timeDF.withColumn("status",
      when(col("status")===0,lit("inactive"))
      when(col("status")===1,lit("active"))
    )
//    statusDF.show()

    // Write DataFrame to CSV file with specified column names
    val csvfinalDF=statusDF.select(
      col("userID"),
      col("name").as("userName"),
      col("orgid").as("userOrgID"),
      col("orgname").as("userOrgName"),
      col("status").as("userStatus"),
      col("courseID"),
      col("courseName"),
      col("completionStatus"),
      col("completedOn")
    )
    csvfinalDF.write
      .option("header",true)
      .csv("/home/mileenamartin/Documents/UserData")
    csvfinalDF.show()

    // Convert completionStatus to digits
    val finalDF = statusDF.withColumn("completionStatus",
      when(col("completionStatus") === "enrolled", lit(0))
        .when(col("completionStatus") === "in-progress", lit(1))
        .when(col("completionStatus") === "completed", lit(2))
        .otherwise(null)
    )
    finalDF.show()

    //Renaming and selecting only the required columns for mysql
    val resultDF= finalDF.select(
      col("userID").as("userid"),
      col("courseID").as("courseid"),
      col("completionStatus"),
      col("completedOn")
    )
       resultDF.show()


    //Writing the dataframe to a new table in mysql
    resultDF.write
      .format("jdbc")
      .option("url","jdbc:mysql://localhost:3306/task_one")
      .option("dbtable","user_enrollments")
      .option("user","root")
      .option("password","Welcome@123")
      .mode("overwrite")
      .save()

  }

}
