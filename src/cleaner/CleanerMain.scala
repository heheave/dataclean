package cleaner

import _root_.java.io.{File, FileOutputStream, PrintWriter}
import java.util
import java.util.Properties
import javaclz.persist.PersistenceLevel
import javaclz.persist.config.HConf
import javaclz.persist.data.{PersistenceDataJsonWrap, PersistenceData}
import javaclz.persist.opt.{FilePersistenceOpt, MongoPersistenceOpt}
import javaclz.{JsonField, JavaConfigure, JavaV}

import _root_.util.AvgPersistenceUtil
import action.{Actions}
import avgcache.{AvgFactory, AvgCacheManager}

import deviceconfig.{DeviceConfigManangerSink}
import kafka.{SimpleKafkaUtils}
import net.sf.json.JSONObject
import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.{SparkConf}

import org.apache.spark.streaming.{Seconds, StreamingContext}
import persistence.PersistenceSink
import sql.SqlJobMnager


/**
  * Created by xiaoke on 17-5-20.
  */
object CleanerMain {

  private val log = Logger.getLogger(CleanerMain.getClass)

  def main(args: Array[String]): Unit = {
    PropertyConfigurator.configure(JavaV.LOG_PATH)
    val file = new File(JavaV.LOG_PATH)
    val conf = new JavaConfigure()
    conf.readFromXml()
    val deployUrl = if (conf.getString(JavaV.MASTER_HOST) == null) {
        "local[4]"
    } else {
      "spark://%s:7077".format(conf.getString(JavaV.MASTER_HOST))
    }
    log.info("Spark deploy url is: " + deployUrl)
    val interval = conf.getLongOrElse(JavaV.STREAMING_TIME_INTERVAL, 10000)
    val sparkConf = new SparkConf()
    sparkConf.setAppName("Clear Main")
      .setMaster(deployUrl)

    val streamingContext = new StreamingContext(sparkConf, Seconds(interval / 1000))

    val jobSqlManager = new SqlJobMnager(streamingContext.sparkContext, conf)
    jobSqlManager.start()

    val realtimeTopic = conf.getStringOrElse(JavaV.SPARKSTREAMING_CLEANED_TOPIC, "cleaned-data-topic")
    val avgDataTopic = conf.getStringOrElse(JavaV.SPARKSTREAMING_AVG_TOPIC, "cleaned-avg-topic")
    val kafkaProducerBroadcast = {
      val kafkaSink = SimpleKafkaUtils.getKafkaSink(conf)
      streamingContext.sparkContext.broadcast(kafkaSink)
    }

    val avgCacheBroadcast = {
      streamingContext.sparkContext.broadcast(
      AvgCacheManager())
    }

    val dbname = conf.getStringOrElse(JavaV.PERSIST_MONGODB_DBNAME, "device")
    val realtimeTblName = conf.getStringOrElse(JavaV.PERSIST_MONGODB_REALTIME_TBLNAME, "realtime")
    val fileRealtimeBasePath = conf.getStringOrElse("xxxxx", "file:///tmp/rdd/realtime/")

    //TODO store the avg info in a same table
    val minAvgTblName = conf.getStringOrElse(JavaV.PERSIST_MONGODB_AVG_TBLNAME, "avgdata")
    val fileAvgBasePath = conf.getStringOrElse("xxxxx", "file:///tmp/rdd/avg/")

//    val hourAvgTblName = conf.getStringOrElse(JavaV.MONGODB_TBLNAME, "houravg")
//    val mongoHourAvgPersistence = new MongoPersistenceOpt(dbname, hourAvgTblName)
//
//    val dayAvgTblName = conf.getStringOrElse(JavaV.MONGODB_TBLNAME, "dayavg")
//    val mongoDayAvgPersistence = new MongoPersistenceOpt(dbname, dayAvgTblName)

    val deviceConfigBroadcast = {
      val properties = new Properties()
      streamingContext.sparkContext.broadcast(DeviceConfigManangerSink(properties))
    }

    val persistenceBroadcast = {
      val properties = new Properties()
      val dbhost = conf.getString(JavaV.PERSIST_MONGODB_HOST)
      if (dbhost != null) properties.put(JavaV.PERSIST_MONGODB_HOST, dbhost)
      val dbport = conf.getString(JavaV.PERSIST_MONGODB_PORT)
      if (dbport != null) properties.put(JavaV.PERSIST_MONGODB_PORT, dbport)
      if (dbname != null) properties.put(JavaV.PERSIST_MONGODB_DBNAME, dbname)
      val hconf = new HConf(streamingContext.sparkContext.hadoopConfiguration)
      val persistenceSink = PersistenceSink(hconf, properties)
      streamingContext.sparkContext.broadcast(persistenceSink)
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
          val producer = kafkaProducerBroadcast.value
          log.info("------------B producer------------")
          val avgCacheManager = avgCacheBroadcast.value
          log.info("------------B avgCacheManager------------")
          val persistence = persistenceBroadcast.value
          log.info("------------B persistence------------")
          val configMap = deviceConfigBroadcast.value.configMap
          log.info("------------B configMap------------")
          implicit val jsonToPd = (jo: JSONObject) => new PersistenceDataJsonWrap(jo)
          val realtimeObjs = new util.LinkedList[PersistenceData]()
          val avgObjs = new util.LinkedList[PersistenceData]()
          val realtimePersistOpt = new MongoPersistenceOpt(dbname, realtimeTblName, new FilePersistenceOpt(fileRealtimeBasePath, null))
          val avgPersistOpt = new MongoPersistenceOpt(dbname, minAvgTblName, new FilePersistenceOpt(fileAvgBasePath, null))
          while (iter.hasNext) {
            val msg = iter.next()._2
            val msgJson = JSONObject.fromObject(msg)
            val id = msgJson.getString(JsonField.DeviceValue.ID)
            val portNum = msgJson.getInt(JsonField.DeviceValue.PORTNUM)
            val dTimeStamp = msgJson.getLong(JsonField.DeviceValue.DTIMESTAMP)
            val values = msgJson.getJSONArray(JsonField.DeviceValue.VALUES)
            val ptimeStamp = msgJson.getLong(JsonField.DeviceValue.PTIMESTAMP)
            var portIdx = 0
            while (portIdx < portNum) {
              val valueJson = values.getJSONObject(portIdx)
              val value = valueJson.get(JsonField.DeviceValue.VALUE)
              val action = configMap.getActions(id, portIdx)
              if (action != null && value != null) {
                log.info("id: " + id + ", action: " + action.getClass.getName + ", type: " + action.avgType())
                val transferedV = action.transferedV(Actions.objToDouble(value))
                valueJson.put(JsonField.DeviceValue.VALUE, transferedV)
                //log.info("------------xxxxxxxxxx---" + (action.avgType & Actions.AVG_MIN))
                val avgs = action.avgType()
                avgs.foreach(avg => {
                  val cinfo = avgCacheManager.addData(id, portIdx, ptimeStamp, transferedV, avg)
                  if (cinfo != null) {
                    val avgObj = AvgPersistenceUtil.avgPersistenceObj(id, portIdx, avg.avgName(),
                      dTimeStamp, ptimeStamp, cinfo.sumData, cinfo.sumNum)
                    avgObjs.add(avgObj)
                    producer.send(avgDataTopic, avgObj.toString)
                  }
                })

//                if (action.avgType != null) {
//                  //log.info("------------xxxxxxxxxx---" + minAvgCache.avgCache.avgMap.keys())
//                  val info = minAvgCache.addData(id, portIdx, ptimeStamp, transferedV)
//                  if (info != null) {
//                    log.info("----------AVG_MIN-------------: " + info.sumData + "---------" + info.sumNum)
//                    val minAvgObj = AvgPersistenceUtil.avgPersistenceObj(
//                      id, portIdx, minAvgCache.avgName, dTimeStamp, ptimeStamp, info.sumData, info.sumNum)
//                    avgObjs.add(minAvgObj)
//                    producer.send(avgDataTopic, minAvgObj.toString)
//                  }
//                }
//
//                if ((action.avgType & Actions.AVG_HOUR) != 0) {
//                  val info = hourAvgCache.addData(id, portIdx, ptimeStamp, transferedV)
//                  if (info != null) {
//                    log.info("----------AVG_HOUR-------------: " + info.sumData + "---------" + info.sumNum)
//                    val hourAvgObj = AvgPersistenceUtil.avgPersistenceObj(
//                      id, portIdx, minAvgCache.avgName, dTimeStamp, ptimeStamp, info.sumData, info.sumNum)
//                    avgObjs.add(hourAvgObj)
//                    producer.send(avgDataTopic, hourAvgObj.toString)
//                  }
//                }
//
//                if ((action.avgType() & Actions.AVG_DAY) != 0) {
//                  val info = dayAvgCache.addData(id, portIdx, ptimeStamp, transferedV)
//                  if (info != null) {
//                    log.info("----------AVG_DAY-------------: " + info.sumData + "---------" + info.sumNum)
//                    val dayAvgObj = AvgPersistenceUtil.avgPersistenceObj(
//                      id, portIdx, minAvgCache.avgName, dTimeStamp, ptimeStamp, info.sumData, info.sumNum)
//                    avgObjs.add(dayAvgObj)
//                    producer.send(avgDataTopic, dayAvgObj.toString)
//                  }
//                }
              }
              //printer.println(valueJson.toString())
              portIdx += 1
            }

            //printer.close()
            realtimeObjs.add(msgJson)
            producer.send(realtimeTopic, msgJson.toString())
            //printer.println(msgJsonStr)
            //deviceConfigBroadcast.destroy()

          }
          log.info("---------------------AVG----------------" + realtimeObjs.size() + realtimePersistOpt.getStr1 + realtimePersistOpt.getStr2)
          persistence.batch(realtimeObjs, realtimePersistOpt, PersistenceLevel.BOTH)
          log.info("---------------------AVG----------------" + avgObjs.size() + avgPersistOpt.getStr1 + avgPersistOpt.getStr2)
          persistence.batch(avgObjs, avgPersistOpt)
          //printer.close()
        } else {
          log.info("Received message is empty: " + System.currentTimeMillis())
        }
      })
      //rdd.saveAsTextFile("/tmp/rdd/%s".format(System.currentTimeMillis()))
    })
    streamingContext.start()
    streamingContext.awaitTermination()
    jobSqlManager.stop()
  }
}
