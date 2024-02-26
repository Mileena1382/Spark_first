ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.12"

//lazy val root = (project in file("."))
//  .settings(
//    name := "SparkTwo"
//  )
val sparkVersion="2.4.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.28"