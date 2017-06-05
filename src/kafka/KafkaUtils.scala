package kafka

import java.util.Properties
import javaclz.{JavaV, JavaConfigure}

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.StreamingContext

/**
  * Created by xiaoke on 17-6-1.
  */
object SimpleKafkaUtils {

  def getStream(streamingContext: StreamingContext, conf: JavaConfigure) = {
    val props = Map[String, String] (
      // zookeeper 配置
      "metadata.broker.list" -> conf.getStringOrElse(JavaV.KAFKA_ZK_URL,
        "192.168.1.110:9092"),
      "zookeeper.connect" -> conf.getStringOrElse(JavaV.KAFKA_ZK_URL,
        "192.168.1.110:2181"),
      // group 代表一个消费组
      "group.id" -> conf.getStringOrElse(JavaV.KAFKA_GROUP_ID,
        "devicegate-group-id"),
      // zk连接超时
      "zookeeper.session.timeout.ms" -> conf.getStringOrElse(JavaV.KAFKA_ZK_SESSION_TIMEOUT, "4000"),
      "zookeeper.sync.time.ms" -> conf.getStringOrElse(JavaV.KAFKA_ZK_SYNC_TIME, "200"),
      "rebalance.max.retries" ->  conf.getStringOrElse(JavaV.KAFKA_REBALANCE_MAX_RETRIES, "5"),
      "rebalance.backoff.ms" ->  conf.getStringOrElse(JavaV.KAFKA_REBALANCE_BACKOFF, "1200"),
      "auto.commit.interval.ms" ->  conf.getStringOrElse(JavaV.KAFKA_AUTO_COMMIT_INTERVAL, "1000"),
      "auto.offset.reset" -> "smallest"
      // 序列化类
      //"serializer.class" -> "kafka.serializer.StringEncoder"
    )
    val topic = Set[String](conf.getStringOrElse(JavaV.KAFKA_TOPIC, "devicegate-topic"))
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder , StringDecoder](streamingContext, props, topic)
    sys.addShutdownHook({
      kafkaStream.stop()
    })
    kafkaStream
  }

  def getKafkaSink(conf: JavaConfigure) = {
    val kafkaProducerConf = new Properties()
    val serverUrl = conf.getStringOrElse(JavaV.KAFKA_SERVER_URL, "192.168.1.110:9092")
    kafkaProducerConf.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverUrl)
    kafkaProducerConf.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    kafkaProducerConf.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    val producerTopic = conf.getStringOrElse(JavaV.SPARKSTREAMING_CLEANED_TOPIC, "cleaned-data-topic")
    val kafkaSink = KafkaSink(producerTopic, kafkaProducerConf)
    kafkaSink
  }

}
