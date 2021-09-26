name := "kafka_client"

version := "0.1"

scalaVersion := "2.12.10"
val sparkVersion = "3.1.0"
val postgresVersion = "42.2.2"
val scalaTestVersion = "3.2.1"

libraryDependencies ++= Seq(
  // logging
  "org.apache.logging.log4j" % "log4j-api" % "2.4.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.4.1",
  "org.apache.kafka" % "kafka-clients" % "2.6.0",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.0",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.12.0",
  "org.json4s" %% "json4s-jackson" % "3.6.11",

  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Test,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % Test,
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-hive" % sparkVersion % Test,
  "org.apache.spark" %% "spark-hive" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion % Test classifier "tests",

  "org.postgresql" % "postgresql" % postgresVersion,
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test,


)
