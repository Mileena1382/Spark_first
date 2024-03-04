package com.example

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object Analysis1 {

  def yamahaCounts(data: DataFrame): Long = {
    data.filter(
      col("brand") === "Yamaha" &&
        col("power").cast("int") > 150 &&
        col("owner") === "First Owner"
    ).count()
  }

  def RTRrows(data:DataFrame):Long={
    data.filter(
      col("bike_name")==="TVS Apache RTR 160cc"
    ).count()
  }
  def bikesInMumbai(data:DataFrame):DataFrame={
    data.filter(
      col("city")==="Mumbai" &&
        col("price").cast("int")<50000 &&
        col("kms_driven").cast("int")<20000
    )


  }

}
