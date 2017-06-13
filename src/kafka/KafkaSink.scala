package kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


/**
  * Created by xiaoke on 17-5-24.
  */
class KafkaSink(createProducer: () => KafkaProducer[String, String]) extends Serializable {

  lazy val producer = createProducer()

  def send(topic: String, value: String) : Unit = {
    producer.send(new ProducerRecord(topic, value))
  }

}

object KafkaSink {
  def apply(conf: Properties) : KafkaSink = {
    val f = () => {
      val producer = new KafkaProducer[String, String](conf)
      sys.addShutdownHook {
        producer.close()
      }
      producer
    }
    new KafkaSink(f)
  }
}
