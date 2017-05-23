package cleaner

import _root_.java.io.{File, FileOutputStream, PrintWriter}
import java.util.Properties
import javaclz.{JavaConfigure, JavaV}

import kafka.serializer.StringDecoder
import net.sf.json.JSONObject
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}



/**
  * Created by xiaoke on 17-5-20.
  */
object CleanerMain {

  private val log = Logger.getLogger(CleanerMain.getClass)

  def main(args: Array[String]): Unit = {
    PropertyConfigurator.configure(JavaV.LOG_PATH)
    val file = new File(JavaV.LOG_PATH)
    println(file.getAbsolutePath)
    println("Hello scala" + JavaV.LOG_PATH)
    val conf = new JavaConfigure()
    conf.readFromXml();
    val masterUrl = "spark://%s:7077".format(conf.getStringOrElse(JavaV.MASTER_HOST, "localhost"))
    log.info("Spark master is:" + masterUrl)
    val interval = conf.getLongOrElse(JavaV.STREAMING_TIME_INTERVAL, 10000)
    val sparkConf = new SparkConf()
    sparkConf.setAppName("Clear Main")
      .setMaster(masterUrl)
    val streamingContext = new StreamingContext(sparkConf, Seconds(interval / 1000))
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

    log.info(classOf[StringSerializer].getName)

    val kafkaProducerBroadcast = {
      val kafkaProducerConf = new Properties()
      val serverUrl = conf.getStringOrElse(JavaV.KAFKA_SERVER_URL, "192.168.1.110:9092")
      kafkaProducerConf.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverUrl)
      kafkaProducerConf.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
      kafkaProducerConf.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
      val producer = new KafkaProducer[String, String](kafkaProducerConf)
      val producerTopic = conf.getStringOrElse(JavaV.SPARKSTREAMING_CLEANED_TOPIC, "cleaned-data-topic")
      streamingContext.sparkContext.broadcast((producerTopic, producer))
    }
    //kafkaStream.
    kafkaStream.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        if (iter.hasNext) {
          val file = new File("/tmp/kafka/kafkatest%s.txt".format("" + System.currentTimeMillis()))
          val printer = new PrintWriter(new FileOutputStream(file, true))
          val topic = kafkaProducerBroadcast.value._1
          val producer = kafkaProducerBroadcast.value._2
          while (iter.hasNext) {
            val msg = iter.next()
            val msg1 = msg._1
            val msg2 = msg._2
            val msgJson = JSONObject.fromObject(msg2)
            val id = msgJson.get("id")
            val portNum = msgJson.getInt("portNum")
            val dTimeStamp = msgJson.getLong("dtimstamp")
            val values = msgJson.getJSONArray("values")
            val ptimeStamp = System.currentTimeMillis()
            var valuesIdx = 0
            while (valuesIdx < portNum) {
              val valueJson = values.getJSONObject(valuesIdx)
              if (!valueJson.isEmpty) {
                valueJson.put("portid", "%s-%03d".format(id, valuesIdx))
                valueJson.put("dtimestamp", dTimeStamp)
                valueJson.put("ptimestamp" , ptimeStamp)
                producer.send(new ProducerRecord[String, String](topic, valueJson.toString()))
              }
              printer.println(valueJson.toString())
              valuesIdx += 1
            }
            printer.close()
          }
        } else {
          log.info("Received message is empty: " + System.currentTimeMillis())
        }
      })
    })
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
