import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.json4s.jackson.Serialization
import org.json4s.{DefaultFormats, Formats}

import java.io.{File, FileReader}
import java.nio.file.Path
import java.util.Properties

object Producer extends App {

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:29092")

  val producer = new KafkaProducer(props, new StringSerializer, new StringSerializer)

  import org.apache.commons.csv.CSVFormat
  implicit val formats: Formats = DefaultFormats

//  val in = new FileReader("C:\\text\\otus\\kafka_scala_example\\kafka_client\\src\\main\\resources\\bestsellers_with_categories.csv")
  val file = new File(getClass.getResource("bestsellers_with_categories.csv").getPath)
  val in = new FileReader(file)
  import scala.collection.JavaConverters._
  val records = CSVFormat
    .RFC4180
    .withHeader("Name", "Author", "User Rating", "Reviews", "Price", "Year", "Genre")
    .withSkipHeaderRecord()
    .parse(in)
    .iterator()
    .asScala
    .toList

  in.close()

  val messages = records.map(record => Serialization.write(BestsellersWithCategories(
    record.get("Name"),
    record.get("Author"),
    record.get("User Rating"),
    record.get("Reviews"),
    record.get("Price").toDouble,
    record.get("Year"),
    record.get("Genre")
  )))

  messages.foreach { m =>
    producer.send(new ProducerRecord("books", m))
  }

  producer.close()

}
