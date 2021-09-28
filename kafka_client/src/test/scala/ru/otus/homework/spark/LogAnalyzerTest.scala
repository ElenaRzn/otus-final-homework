package ru.otus.homework.spark

import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.SharedSparkSession
import ru.otus.homework.data.dto.{LogRecord, Scoring}

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

  test("errorsStatistics") {
    val ds = Seq(
      generateEmptyScor(1632577001461L, 1632577001463L),
      generateEmptyScor(1632577001461L, 1632577001463L),
      generateEmptyScor(1632577001461L, 1632577001463L),
      generateErrorScor(1632577001463L, 1632577002845L, 1632577001463L, "IGNITE_EXCEPTION", 1632577001461L, 1632577001463L),
      generateErrorScor(1632577001463L, 1632577005555L, 1632577001463L, "IGNITE_EXCEPTION", 1632577001461L, 1632577001463L),
      generateErrorScor(1632577001463L, 1632577006767L, 1632577001463L, "MODEL_EXCEPTION", 1632577001461L, 1632577001463L),
      generateErrorScor(1632577001463L, 1632577003434L, 1632577001463L, "MODEL_EXCEPTION", 1632577001461L, 1632577001463L),
      generateErrorScor(1632577001463L, 1632577008756L, 1632577001463L, "MODEL_EXCEPTION", 1632577001461L, 1632577001463L),
      generateErrorScor(1632577001463L, 1632577005643L, 1632577001463L, "MODEL_EXCEPTION", 1632577001461L, 1632577001463L)

    ).toDS

    val result = LogsAnalyzer.errorsStatistics(ds)

    checkAnswer(
      result,
      Row("MODEL_EXCEPTION", 8, 1971, 7293, 4687.0) :: Row("IGNITE_EXCEPTION", 4, 1382, 4092, 2737.0) :: Nil)
  }

  test("emptyStatistics") {
    val ds = Seq(
      generateEmptyScor(1632577001461L, 1632577001463L),
      generateEmptyScor(1632577001461L, 1632577001463L),
      generateEmptyScor(1632577001461L, 1632577001463L),
      generateErrorScor(1632577001463L, 1632577002845L, 1632577001463L, "IGNITE_EXCEPTION", 1632577001461L, 1632577001463L),
      generateErrorScor(1632577001463L, 1632577005555L, 1632577001463L, "IGNITE_EXCEPTION", 1632577001461L, 1632577001463L),
      generateErrorScor(1632577001463L, 1632577006767L, 1632577001463L, "MODEL_EXCEPTION", 1632577001461L, 1632577001463L),
      generateErrorScor(1632577001463L, 1632577003434L, 1632577001463L, "MODEL_EXCEPTION", 1632577001461L, 1632577001463L),
      generateErrorScor(1632577001463L, 1632577008756L, 1632577001463L, "MODEL_EXCEPTION", 1632577001461L, 1632577001463L),
      generateErrorScor(1632577001463L, 1632577005643L, 1632577001463L, "MODEL_EXCEPTION", 1632577001461L, 1632577001463L)

    ).toDS

    val result = LogsAnalyzer.emptyStatistics(ds)

    println("******************************")
    result.printSchema()
    result.show
    println("******************************")

    checkAnswer(
      result,
      Row(3, 2, 2, 2.0) :: Nil
    )
  }
}
