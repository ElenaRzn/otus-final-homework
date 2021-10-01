package ru.otus.homework.data.kafka

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import org.slf4j.LoggerFactory
import ru.otus.homework.data.dto.Full

import java.util.Properties
import scala.io.Source

/***
 * Утилитный класс для записи в кафку.
 * Не используется в "продакшн" режиме.
 */
object Producer extends App {
  implicit val formats = Serialization.formats((NoTypeHints))
  val log = LoggerFactory.getLogger(Consumer.getClass)

  val full = readFromJsonFile("C:\\text\\otus\\kafka_scala_example\\kafka_client\\src\\main\\resources\\tx-logs.json")
  sendToKafka(full.logs.map(write(_)))
  log.debug("done")

  def sendToKafka(records: List[String]): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:29092")
    val producer = new KafkaProducer(props, new StringSerializer, new StringSerializer)
    records.foreach { m =>
      println(m)
      producer.send(new ProducerRecord("tx_logs", m))
    }
    producer.close()
  }

  def readFromJsonFile(filename: String): Full = {

    log.debug(s"Reading $filename ...")

    val json = Source.fromFile(filename)
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    val records = mapper.readValue[Full](json.reader())

    log.debug(s"Has read ${records.logs.length} records")
    records
  }





}
