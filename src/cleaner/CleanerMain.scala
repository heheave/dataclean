package cleaner

import _root_.java.io.{File, FileOutputStream, PrintWriter}
import java.util
import java.util.Properties
import javaclz.persist.data.{PersistenceDataJsonWrap, PersistenceData}
import javaclz.persist.opt.MongoPersistenceOpt
import javaclz.{JsonField, JavaConfigure, JavaV}

import action.{Actions}
import avgcache.{AvgCacheManager}

import config.{DeviceZkConfigManangerSink, DeviceConfigManager}
import kafka.{SimpleKafkaUtils}
import net.sf.json.JSONObject
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.scheduler.{StreamingListenerBatchSubmitted, StreamingListener}

import org.apache.spark.streaming.{Seconds, StreamingContext}
import persistence.PersistenceSink


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
    conf.readFromXml()
    val deployUrl = if (conf.getString(JavaV.MASTER_HOST) == null) {
        "local[2]"
    } else {
      "spark://%s:7077".format(conf.getString(JavaV.MASTER_HOST))
    }
    log.info("Spark deploy url is: " + deployUrl)
    val interval = conf.getLongOrElse(JavaV.STREAMING_TIME_INTERVAL, 10000)
    val sparkConf = new SparkConf()
    sparkConf.setAppName("Clear Main")
      .setMaster(deployUrl)
    val streamingContext = new StreamingContext(sparkConf, Seconds(interval / 1000))


    log.info(classOf[StringSerializer].getName)

    val kafkaProducerBroadcast = {
      val kafkaSink = SimpleKafkaUtils.getKafkaSink(conf)
      streamingContext.sparkContext.broadcast(kafkaSink)
    }

    val avgCacheBroadcast = {
      streamingContext.sparkContext.broadcast(
      AvgCacheManager.applyDay(), AvgCacheManager.applyHour(), AvgCacheManager.applyMin())
    }

    val dbname = conf.getStringOrElse(JavaV.MONGODB_DBNAME, "device")
    val tblname = conf.getStringOrElse(JavaV.MONGODB_TBLNAME, "realtime")
    val mongoPersistence = new MongoPersistenceOpt(dbname, tblname)
    val persistenceBroadcast = {
      val properties = new Properties()
      val dbhost = conf.getString(JavaV.MONGODB_HOST)
      if (dbhost != null) properties.put(JavaV.MONGODB_HOST, dbhost)
      val dbport = conf.getString(JavaV.MONGODB_PORT)
      if (dbport != null) properties.put(JavaV.MONGODB_PORT, dbport)
      if (dbname != null) properties.put(JavaV.MONGODB_DBNAME, dbname)
      val persistenceSink = PersistenceSink(properties)
      streamingContext.sparkContext.broadcast(persistenceSink)
    }

    val deviceConfigBroadcast = {
      val properties = new Properties()
      streamingContext.sparkContext.broadcast(DeviceZkConfigManangerSink(properties))
    }

    val kafkaStream = SimpleKafkaUtils.getStream(streamingContext, conf)

//    streamingContext.addStreamingListener(new StreamingListener {
//      override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted): Unit = {
//        log.info("batch submitted --------------------------")
//        if (deviceConfigBroadcast != null) {
//          deviceConfigBroadcast.unpersist()
//        }
//        deviceConfigBroadcast = {
//          val configMap = DeviceConfigManager(conf).configFromPath()
//          //configMap.append("ANALOG-A01A244", -1, MulAction(0))
//          streamingContext.sparkContext.broadcast(configMap)
//        }
//        log.info("rebroadcast deviceConfigBroadcast --------------------------")
//      }
//    })

    kafkaStream.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        if (iter.hasNext) {
          //val file = new File("/tmp/kafka/kafkatest%s.txt".format("" + System.currentTimeMillis()))
          //val printer = new PrintWriter(new FileOutputStream(file, true))
          val producer = kafkaProducerBroadcast.value
          val dayAvgCache = avgCacheBroadcast.value._1
          val hourAvgCache = avgCacheBroadcast.value._2
          val minAvgCache = avgCacheBroadcast.value._3
          val persistence = persistenceBroadcast.value
          val configMap = deviceConfigBroadcast.value.configMap

          implicit val jsonToPd = (jo: JSONObject) => new PersistenceDataJsonWrap(jo)
          val listPd = new util.LinkedList[PersistenceData]()
          while (iter.hasNext) {
            val msg = iter.next()._2
            val msgJson = JSONObject.fromObject(msg)
            val id = msgJson.getString(JsonField.DeviceValue.ID)
            val portNum = msgJson.getInt(JsonField.DeviceValue.PORTNUM)
            val dTimeStamp = msgJson.getLong(JsonField.DeviceValue.DTIMESTAMP)
            val values = msgJson.getJSONArray(JsonField.DeviceValue.VALUES)
            val ptimeStamp = System.currentTimeMillis()
            msgJson.put(JsonField.DeviceValue.PTIMESTAMP, ptimeStamp)
            var portIdx = 0
            while (portIdx < portNum) {
              val valueJson = values.getJSONObject(portIdx)
//              if (!valueJson.isEmpty) {
//                valueJson.put(JsonField.DeviceValue.ID, id)
//                valueJson.put(JsonField.DeviceValue.PORTID, "%s-%03d".format(id, valuesIdx))
//                valueJson.put(JsonField.DeviceValue.DTIMESTAMP, dTimeStamp)
//                valueJson.put(JsonField.DeviceValue.PTIMESTAMP , ptimeStamp)
//                valueJson.put(JsonField.DeviceValue.VALUE, msgJson.get(JsonField.DeviceValue.VALUE))
//                valueJson.put(JsonField.DeviceValue.UNIT, msgJson.get(JsonField.DeviceValue.UNIT))
//                valueJson.put(JsonField.DeviceValue.VALID, msgJson.get(JsonField.DeviceValue.VALID))
//                producer.send(valueJson.toString)
//              }
              val value = valueJson.get(JsonField.DeviceValue.VALUE)
              val action = configMap.getActions(id, portIdx)
              if (action != null && value != null) {
                log.info("id: " + id + ", action: " + action.getClass.getName + ", type: " + action.avgType())
                val transferedV = action.transferedV(Actions.objToDouble(value))
                valueJson.put(JsonField.DeviceValue.VALUE, transferedV)
                //log.info("------------xxxxxxxxxx---" + (action.avgType & Actions.AVG_MIN))
                if ((action.avgType & Actions.AVG_MIN) != 0) {
                  //log.info("------------xxxxxxxxxx---" + minAvgCache.avgCache.avgMap.keys())
                  val info = minAvgCache.addData(id, portIdx, ptimeStamp, transferedV)
                  if (info != null) {
                    log.info("----------AVG_MIN-------------: " + info.sumData + "---------" + info.sumNum)
                  }
                }

                if ((action.avgType & Actions.AVG_HOUR) != 0) {
                  val info = hourAvgCache.addData(id, portIdx, ptimeStamp, transferedV)
                  if (info != null) {
                    log.info("----------AVG_HOUR-------------: " + info.sumData + "---------" + info.sumNum)
                  }
                }

                if ((action.avgType() & Actions.AVG_DAY) != 0) {
                  val info = dayAvgCache.addData(id, portIdx, ptimeStamp, transferedV)
                  if (info != null) {
                    log.info("----------AVG_DAY-------------: " + info.sumData + "---------" + info.sumNum)
                  }
                }
              }
              //printer.println(valueJson.toString())
              portIdx += 1
            }

            //printer.close()
            val msgJsonStr = msgJson.toString()
            producer.send(msgJsonStr)
            listPd.add(msgJson)
            //printer.println(msgJsonStr)
            //deviceConfigBroadcast.destroy()
          }
          persistence.batch(listPd, mongoPersistence)
          //printer.close()
        } else {
          log.info("Received message is empty: " + System.currentTimeMillis())
        }
      })
    })
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
