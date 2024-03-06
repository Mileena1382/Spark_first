package com.example

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaSparkBasic {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    // Initialize SparkSession
    val spark = SparkSession
      .builder()
      .appName("KafkaSparkIntegration")
      .master("local[*]")
      .getOrCreate()


    //Initialize variable stream with a batch interval of 5 seconds

    val streamingContext=new StreamingContext(spark.sparkContext, Seconds(5))

    // Kafka Parameters
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test-group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    //Subscribe to Kafka topics

    val topics = Array("quickstart-events")
    val stream=KafkaUtils.createDirectStream[String,String](
      streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    //Process Kafka Stream
    stream.foreachRDD { rdd =>
      rdd.foreach { record =>
        println(s"Message is: ${record.value}")
      }
    }


    streamingContext.start()
    streamingContext.awaitTermination()


  }

}

