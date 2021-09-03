import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConverters._
import java.util.{Collections, Properties}
import java.time.Duration

object Consumer extends App {

  val props = new Properties()
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092")
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "homework")

  val consumer = new KafkaConsumer(props, new StringDeserializer, new StringDeserializer)
  consumer.subscribe(Collections.singletonList("books"))

  val partitions = consumer.partitionsFor("books")
    .asScala
    .map(x=> new TopicPartition(x.topic(), x.partition()))
    .toList.asJava

  val endOffsets = consumer.endOffsets(partitions)

  consumer.unsubscribe()
  consumer.assign(partitions)
  partitions.forEach(part => {
    consumer.seek(part, endOffsets.get(part) - 5)
  })

  val data = consumer
    .poll(Duration.ofSeconds(10))
    .asScala

  println(s"data size = ${data.size}")
  data.foreach(println(_))

  consumer.close()

}
