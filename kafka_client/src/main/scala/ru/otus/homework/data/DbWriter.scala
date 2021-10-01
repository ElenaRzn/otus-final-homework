package ru.otus.homework.data

import org.apache.spark.sql.{DataFrame, SaveMode}

import java.util.Properties

object DbWriter {
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/otus"
  val user = "docker"
  val password = "docker"

  val connectionProperties = new Properties()

  connectionProperties.put("user", user)
  connectionProperties.put("password", password)

  def write(df: DataFrame, tableName: String) = {
    df.write.jdbc(url=url, table=tableName, connectionProperties=connectionProperties)
  }

}
