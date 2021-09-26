package ru.otus.homework.spark

import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, explode, flatten}
import org.apache.spark.sql.test.SharedSparkSession
import ru.otus.homework.data.dto.{LogRecord, Scoring}
import ru.otus.homework.spark.LogsAnalyzer

import java.util.UUID

class LogAnalyzerTest extends SharedSparkSession{
  import testImplicits._

  def generateEmptyScor(timeReceive: Long, timeFinished: Long) = {
    LogRecord(
      event_id = UUID.randomUUID().toString,
      scorings = None,
      timeReceive = timeReceive,
      timeFinished = timeFinished,
      errorData = None
    )
  }

  def generateErrorScor(timeAskIgnite: Long,
                        timeReceiveAnswerForIgnite: Long,
                        timeReceiveDataForModel: Long,
                        errorType: String,
                        timeReceive: Long,
                        timeFinished: Long) = {
    val scor = Scoring(
      name = "SCORING_RB",
      score = -1,
      instance = "NULL",
      version = -1,
      build = -1,
      timeAskIgnite = timeAskIgnite,
      timeReceiveAnswerForIgnite = timeReceiveAnswerForIgnite,
      timeReceiveDataForModel = timeReceiveDataForModel,
      errorType = errorType,
      errorData = None
    )
    val scor2 = Scoring(
      name = "DDDDDD",
      score = -1,
      instance = "NULL",
      version = -1,
      build = -1,
      timeAskIgnite = timeAskIgnite,
      timeReceiveAnswerForIgnite = timeReceiveAnswerForIgnite,
      timeReceiveDataForModel = timeReceiveDataForModel,
      errorType = errorType,
      errorData = None
    )
    LogRecord(
      event_id = UUID.randomUUID().toString,
      scorings = Some(List(scor, scor2)),
      timeReceive = timeReceive,
      timeFinished = timeFinished,
      errorData = None
    )
  }

  test("join - join using") {
    val ds = Seq(
      generateEmptyScor(1632577001461L, 1632577001463L),
      generateEmptyScor(1632577001461L, 1632577001463L),
      generateEmptyScor(1632577001461L, 1632577001463L),
      generateErrorScor(1632577001463L, 1632577001463L, 1632577001463L, "IGNITE_EXCEPTION", 1632577001461L, 1632577001463L),
      generateErrorScor(1632577001463L, 1632577001463L, 1632577001463L, "IGNITE_EXCEPTION", 1632577001461L, 1632577001463L)

    ).toDS
    println("///////////////////////")
    val fff = ds.select(col("scorings")).where(col("scorings").isNotNull).select(explode(col("scorings")).as("ddd")).select(col("ddd.*")).printSchema()
    println("///////////////////////")
//    ds.select(col("scorings")).where(col("scorings").isNotNull).select(explode(col("scorings")).as("ddd")).printSchema()
//    val result = LogsAnalyzer.calculateErrors(ds)
//
//    checkAnswer(
//      result,
//      Row(1, "1", "2") :: Row(2, "2", "3") :: Row(3, "3", "5") :: Nil)
  }
}
