package kafka

import java.util.{UUID, Properties}
import javaclz.{JavaV, JavaConfigure}

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.storage.StorageLevel
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
      "metadata.broker.list" -> conf.getStringOrElse(JavaV.KAFKA_SERVER_URL),
      "zookeeper.connect" -> conf.getStringOrElse(JavaV.KAFKA_ZK_URL),
      // group 代表一个消费组
      "group.id" -> conf.getStringOrElse(JavaV.KAFKA_GROUP_ID),
      // zk连接超时
      "zookeeper.session.timeout.ms" -> conf.getStringOrElse(JavaV.KAFKA_ZK_SESSION_TIMEOUT),
      "zookeeper.sync.time.ms" -> conf.getStringOrElse(JavaV.KAFKA_ZK_SYNC_TIME),
      "rebalance.max.retries" ->  conf.getStringOrElse(JavaV.KAFKA_REBALANCE_MAX_RETRIES),
      "rebalance.backoff.ms" ->  conf.getStringOrElse(JavaV.KAFKA_REBALANCE_BACKOFF),
      "auto.commit.interval.ms" ->  conf.getStringOrElse(JavaV.KAFKA_AUTO_COMMIT_INTERVAL),
      "auto.offset.reset" -> conf.getStringOrElse(JavaV.KAFKA_AUTO_OFFSET_RESET)
      // 序列化类
      //"serializer.class" -> "kafka.serializer.StringEncoder"
    )
    val topic = Map[String, Int](conf.getStringOrElse(JavaV.KAFKA_TOPIC) -> 1)
    val parilizeNum = conf.getIntOrElse(JavaV.KAFKA_PARALLELISM_NUM)
    val kafkaStreams = (1 to parilizeNum).map(i => {
      val stream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
        streamingContext, props, topic, StorageLevel.MEMORY_ONLY)
      sys.addShutdownHook(stream.stop())
      stream
    })
    val unionedStream = streamingContext.union(kafkaStreams)
    unionedStream//.repartition(parilizeNum)
  }

  def getKafkaSink(conf: JavaConfigure) = {
    val kafkaProducerConf = new Properties()
    val serverUrl = conf.getStringOrElse(JavaV.KAFKA_SERVER_URL)
    kafkaProducerConf.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverUrl)
    // kafkaProducerConf.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, Int.box(0))
    kafkaProducerConf.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    kafkaProducerConf.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    val kafkaSink = KafkaSink(kafkaProducerConf)
    kafkaSink
  }

}
