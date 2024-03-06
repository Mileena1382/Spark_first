ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.12"

//lazy val root = (project in file("."))
//  .settings(
//    name := "KafkaSparkIntegration"
//  )
val sparkVersion="2.4.1"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion, // Kafka 0.10 integration
  "org.apache.kafka" % "kafka-clients" % "2.8.0" // Kafka client dependency
)
