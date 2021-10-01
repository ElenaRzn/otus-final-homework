package ru.otus.homework.spark

import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.SharedSparkSession
import ru.otus.homework.data.FileReader
import ru.otus.homework.data.dto.{LogRecord, Scoring}

import java.util.UUID

class LogAnalyzerTest extends SharedSparkSession{
  import testImplicits._

  def generateEmptyScor(timeReceive: Long, timeFinished: Long, event_id: Option[String] = None) = {
    LogRecord(
      event_id = event_id.getOrElse(UUID.randomUUID().toString),
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
                        timeFinished: Long,
                        event_id: Option[String] = None) = {
    val scor = Scoring(
      name = "SCORING_RB",
      score = -1,
      instance = "NULL",
      version = -1,
      build = -1,
      timeAskIgnite = timeAskIgnite,
      timeReceiveAnswerForIgnite = timeReceiveAnswerForIgnite,
      timeReceiveDataForModel = timeReceiveDataForModel,
      errorType = Some(errorType),
      errorData = None
    )
    val scor2 = Scoring(
      name = "ANOTHER_TEST_MODEL",
      score = -1,
      instance = "NULL",
      version = -1,
      build = -1,
      timeAskIgnite = timeAskIgnite,
      timeReceiveAnswerForIgnite = timeReceiveAnswerForIgnite,
      timeReceiveDataForModel = timeReceiveDataForModel,
      errorType = Some(errorType),
      errorData = None
    )
    LogRecord(
      event_id = event_id.getOrElse(UUID.randomUUID().toString),
      scorings = Some(List(scor, scor2)),
      timeReceive = timeReceive,
      timeFinished = timeFinished,
      errorData = None
    )
  }

  def generateSuccessScor(timeAskIgnite: Long,
                        timeReceiveAnswerForIgnite: Long,
                        timeReceiveDataForModel: Long,
                          scoreValue: Int,
                        timeReceive: Long,
                        timeFinished: Long,
                        event_id: Option[String] = None) = {
    val scor = Scoring(
      name = "SCORING_RB",
      score = scoreValue,
      instance = "NULL",
      version = -1,
      build = -1,
      timeAskIgnite = timeAskIgnite,
      timeReceiveAnswerForIgnite = timeReceiveAnswerForIgnite,
      timeReceiveDataForModel = timeReceiveDataForModel,
      errorType = None,
      errorData = None
    )
    val scor2 = Scoring(
      name = "ANOTHER_TEST_MODEL",
      score = scoreValue + 10,
      instance = "NULL",
      version = -1,
      build = -1,
      timeAskIgnite = timeAskIgnite,
      timeReceiveAnswerForIgnite = timeReceiveAnswerForIgnite,
      timeReceiveDataForModel = timeReceiveDataForModel,
      errorType = None,
      errorData = None
    )
    LogRecord(
      event_id = event_id.getOrElse(UUID.randomUUID().toString),
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

    println("******************************")
    result.printSchema()
    result.show
    println("******************************")

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

  test("prepareSuccessStatistics") {
    val ds = Seq(
      generateSuccessScor(1632577001463L, 1632577002845L, 1632577001463L, 70, 1632577001461L, 1632577001463L, Some("250921163204566|83697")),
      generateSuccessScor(1632577001463L, 1632577005555L, 1632577001463L, 70, 1632577001461L, 1632577001463L, Some("250921163338840|91878")),
      generateSuccessScor(1632577001463L, 1632577006767L, 1632577001463L, 10, 1632577001461L, 1632577001463L, Some("250921163342488|83872")),
      generateSuccessScor(1632577001463L, 1632577003434L, 1632577001463L, 10, 1632577001461L, 1632577001463L, Some("250921163349845|87917")),
      generateSuccessScor(1632577001463L, 1632577008756L, 1632577001463L, 60, 1632577001461L, 1632577001463L, Some("00p3_0000000000000745931:26044cc21dfd11ec8010c945b3871705")),
      generateSuccessScor(1632577001463L, 1632577005643L, 1632577001463L, 60, 1632577001461L, 1632577001463L, Some("00p3_0000000000000745931:26044cc21dfd11ec8010c945b3871754")),
      generateEmptyScor(1632577001461L, 1632577001463L),
      generateEmptyScor(1632577001461L, 1632577001463L),
      generateErrorScor(1632577001463L, 1632577006767L, 1632577001463L, "MODEL_EXCEPTION", 1632577001461L, 1632577001463L),
      generateErrorScor(1632577001463L, 1632577008756L, 1632577001463L, "MODEL_EXCEPTION", 1632577001461L, 1632577001463L),
    ).toDS

    val fraud = FileReader.readCSV("src/main/resources/fraud.csv")(spark)

    val result = LogsAnalyzer.prepareSuccessStatistics(ds, fraud)

    println("******************************")
    result.printSchema()
    result.show
    println("******************************")

    assert(result.count() == 12)
  }


  test("countTrueFraud") {
    val ds = Seq(
      generateSuccessScor(1632577001463L, 1632577002845L, 1632577001463L, 70, 1632577001461L, 1632577001463L, Some("250921163204566|83697")),
      generateSuccessScor(1632577001463L, 1632577005555L, 1632577001463L, 70, 1632577001461L, 1632577001463L, Some("250921163338840|91878")),
      generateSuccessScor(1632577001463L, 1632577006767L, 1632577001463L, 10, 1632577001461L, 1632577001463L, Some("250921163342488|83872")),
      generateSuccessScor(1632577001463L, 1632577003434L, 1632577001463L, 10, 1632577001461L, 1632577001463L, Some("250921163349845|87917")),
      generateSuccessScor(1632577001463L, 1632577008756L, 1632577001463L, 60, 1632577001461L, 1632577001463L, Some("00p3_0000000000000745931:26044cc21dfd11ec8010c945b3871705")),
      generateSuccessScor(1632577001463L, 1632577005643L, 1632577001463L, 60, 1632577001461L, 1632577001463L, Some("00p3_0000000000000745931:26044cc21dfd11ec8010c945b3871754"))

    ).toDS

    val fraud = FileReader.readCSV("src/main/resources/fraud.csv")(spark)


    val result = LogsAnalyzer.countTrueFraud(LogsAnalyzer.prepareSuccessStatistics(ds, fraud))

    println("******************************")
    result.printSchema()
    result.show
    println("******************************")

    assert(result.count() == 6)
  }

  test("countMissedFraud") {
    val ds = Seq(
      generateSuccessScor(1632577001463L, 1632577002845L, 1632577001463L, 70, 1632577001461L, 1632577001463L, Some("250921163204566|83697")),
      generateSuccessScor(1632577001463L, 1632577005555L, 1632577001463L, 70, 1632577001461L, 1632577001463L, Some("250921163338840|91878")),
      generateSuccessScor(1632577001463L, 1632577006767L, 1632577001463L, 10, 1632577001461L, 1632577001463L, Some("250921163342488|83872")),
      generateSuccessScor(1632577001463L, 1632577008756L, 1632577001463L, 60, 1632577001461L, 1632577001463L, Some("00p3_0000000000000745931:26044cc21dfd11ec8010c945b3871705")),
      generateSuccessScor(1632577001463L, 1632577005643L, 1632577001463L, 60, 1632577001461L, 1632577001463L, Some("00p3_0000000000000745931:26044cc21dfd11ec8010c945b3871754"))

    ).toDS
    val fraud = FileReader.readCSV("src/main/resources/fraud.csv")(spark)


    val result = LogsAnalyzer.countMissedFraud(LogsAnalyzer.prepareSuccessStatistics(ds, fraud))

    println("******************************")
    result.printSchema()
    result.show
    println("******************************")

    assert(result.count() == 4)
  }
}
