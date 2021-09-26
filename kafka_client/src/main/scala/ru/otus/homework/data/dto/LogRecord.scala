package ru.otus.homework.data.dto

case class LogRecord(
                      event_id: String,
                      scorings: Option[List[Scoring]],
                      timeReceive: Long,
                      timeFinished: Long,
                      errorData: Option[String]
                    )

case class Scoring(
                    name: String,
                    score: Int,
                    instance: String,
                    version: Int,
                    build: Int,
                    timeAskIgnite: Long,
                    timeReceiveAnswerForIgnite: Long,
                    timeReceiveDataForModel: Long,
                    errorType: String,
                    errorData: Option[String]
                  )
case class Full(logs: List[LogRecord])