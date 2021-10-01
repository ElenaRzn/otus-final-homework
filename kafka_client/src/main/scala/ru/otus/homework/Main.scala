package ru.otus.homework

import org.apache.kafka.common.record.FileLogInputStream.FileChannelRecordBatch
import org.apache.spark.sql.SparkSession
import org.json4s.NoTypeHints
import ru.otus.homework.data.{DbWriter, FileReader}
import ru.otus.homework.data.kafka.Consumer
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization
import ru.otus.homework.data.dto.LogRecord
import ru.otus.homework.spark.LogsAnalyzer

object Main extends App {
  val spark = SparkSession.builder()
    .appName("tx-logs_analyzer")
    .config("spark.master", "local")
    .getOrCreate()

  val fraud = FileReader.readCSV("C:\\text\\otus\\final\\otus-final-homework\\kafka_client\\src\\main\\resources\\fraud.csv")(spark)

  import spark.implicits._

  implicit val formats = Serialization.formats((NoTypeHints))
  val ds = Consumer.readLogs().map(parse(_).extract[LogRecord]).toList.toDS()

  val errorsStatistics = LogsAnalyzer.errorsStatistics(ds)
  DbWriter.write(errorsStatistics, "errors_statistics")

  val empty = LogsAnalyzer.emptyStatistics(ds)
  DbWriter.write(empty, "empty_statistics")

  val preparedSuccess = LogsAnalyzer.prepareSuccessStatistics(ds, fraud)

  preparedSuccess.persist()
  val missed = LogsAnalyzer.countMissedFraud(preparedSuccess)
  DbWriter.write(missed, "missed_fraud")

  val found = LogsAnalyzer.countTrueFraud(preparedSuccess)
  DbWriter.write(found, "true_fraud")



}
