package ru.otus.homework.data.kafka

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory

import java.time.Duration
import java.util.{Collections, Properties}
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

object Consumer {
  val log = LoggerFactory.getLogger(Consumer.getClass)
  val props = new Properties()
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092")
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "homework")

  def readLogs(): Iterable[String] = {
    val consumer = new KafkaConsumer(props, new StringDeserializer, new StringDeserializer)
    consumer.subscribe(Collections.singletonList("tx_logs"))

    val data = consumer
      .poll(Duration.ofSeconds(10))
      .asScala
      .map(record => record.value())

    println(s"data size = ${data.size}")
    data.foreach(println)

    consumer.close()
    data
  }

}
