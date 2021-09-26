package ru.otus.homework.spark

import org.apache.spark.sql.functions.{col, grouping}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions}
import ru.otus.homework.data.dto.LogRecord

object LogsAnalyzer {

  val spark = SparkSession.builder()
    .appName("tx-logs_analyzer")
    .config("spark.master", "local")
    .getOrCreate()

  //считаем кол-во неуспешных ответов
  def calculateErrors(ds: Dataset[LogRecord]): DataFrame = {
    val err = ds.groupBy(col("scorings")).agg(functions.count(col("event_id")))
    //todo add grouping(col("errorType"))
    err
  }



}
