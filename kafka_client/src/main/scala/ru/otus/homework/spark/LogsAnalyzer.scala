package ru.otus.homework.spark

import org.apache.spark.sql.functions.{broadcast, col, count, explode, grouping}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions}
import ru.otus.homework.data.dto.LogRecord
import ru.otus.homework.data.FileReader

/***
 * 1. посчитать кол-во пустых ответов;
 * 2. посчитать среднее, минимальное и максимальное время ответ;
 * 3. сгруппировать по типам ошибок и посчитать кол-во; минимальное, максимальное время ответа игнайта
 * 4. сджоинить с файлом с ответами и проверить скоринги, где они неошибочные.
 */
object LogsAnalyzer {

  val spark = SparkSession.builder()
    .appName("tx-logs_analyzer")
    .config("spark.master", "local")
    .getOrCreate()

  //считаем кол-во неуспешных ответов
  def emptyStatistics(ds: Dataset[LogRecord]): DataFrame = {
    ds.filter(col("scorings").isNull)
      .withColumn("processing_time", col("timeFinished") - col("timeReceive"))
      //      .groupBy(col("scorings"))
      .agg(functions.count(col("event_id")),
        functions.min("processing_time"),
        functions.max("processing_time"),
        functions.avg("processing_time"))
  }

  def errorsStatistics(ds: Dataset[LogRecord]): DataFrame = {
    ds.select(col("scorings"))
      .where(col("scorings").isNotNull)
      .select(explode(col("scorings")).as("flat_scors"))
      .select(col("flat_scors.*"))
      .withColumn("ignite_response_time", col("timeReceiveAnswerForIgnite") - col("timeAskIgnite"))
      .groupBy(col("errorType"))
      .agg(count("timeReceiveDataForModel"),
        functions.min("ignite_response_time"),
        functions.max("ignite_response_time"),
        functions.avg("ignite_response_time"))
  }

  def prepareSuccessStatistics(ds: Dataset[LogRecord]): DataFrame = {
    val fraud = FileReader.readCSV("src/main/resources/fraud.csv")(spark)

    ds.join(broadcast(fraud), col("event_id") === col("event"))
      .withColumn("flat_scors", explode(col("scorings")))
      .select(col("flat_scors.*"), col("timeReceive"), col("timeFinished"))
      .where(col("flat_scors.score") =!= -1)
      .withColumn("response_time", col("timeFinished") - col("timeReceive"))
  }

  def countTrueFraud(preparedDs: DataFrame): DataFrame = {
    preparedDs.filter(col("score") > 65)
  }

  def countMissedFraud(preparedDs: DataFrame): DataFrame = {
    preparedDs.filter(col("score") <= 65)
  }



}
