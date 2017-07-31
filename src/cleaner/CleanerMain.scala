package cleaner

import _root_.java.io.{File, FileOutputStream, PrintWriter}
import java.util
import java.util.Properties
import java.util.concurrent.atomic.AtomicLong
import javaclz.persist.PersistenceLevel
import javaclz.persist.config.HConf
import javaclz.persist.data.{PersistenceDataJsonWrap, PersistenceData}
import javaclz.persist.opt.{FilePersistenceOpt, MongoPersistenceOpt}
import javaclz.{JsonField, JavaConfigure, JavaV}

import _root_.util.AvgPersistenceUtil
import conf.DeviceConfigManangerSink
import conf.action.{Actions}
import avgcache.{AvgFactory, AvgCacheManager}
import kafka.{SimpleKafkaUtils}
import metric.MessageMetric
import net.sf.json.JSONObject
import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.streaming.scheduler.{StreamingListenerBatchStarted, StreamingListenerBatchCompleted, StreamingListenerReceiverStarted, StreamingListener}
import org.apache.spark.{SparkContext, Logging, SparkConf}

import org.apache.spark.streaming.{Seconds, StreamingContext}
import persistence.PersistenceSink
import sql.SqlJobMnager


/**
  * Created by xiaoke on 17-5-20.
  */
object CleanerMain extends Logging {

  def main(args: Array[String]): Unit = {
    // PropertyConfigurator.configure(JavaV.LOG_PATH)
    // val file = new File(JavaV.LOG_PATH)
    val conf = new JavaConfigure()
    conf.readFromXml()
    val deployUrl = if (conf.getString(JavaV.MASTER_HOST) == null) {
        "local[4]"
    } else {
      "spark://%s:7077".format(conf.getString(JavaV.MASTER_HOST))
    }
    log.info("Spark deploy url is: " + deployUrl)
    val secInterval = conf.getLongOrElse(JavaV.STREAMING_TIME_SEC_INTERVAL, 10)
    val sparkConf = new SparkConf()
    sparkConf.setAppName("Cleaner")
      .setMaster(deployUrl)
      // Can this make a difference ???
      .set("spark.throw.exception.without.resource", "false")
      .set("spark.streaming.unpersist", "true")
    val sparkContext = new SparkContext(sparkConf)

    val streamingContext = new StreamingContext(sparkContext, Seconds(secInterval))

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

    // file backup store in a same palce
    val fileRealtimeBasePath = conf.getStringOrElse(JavaV.PERSIST_FILE_REALTIME_BASEPATH, "file:///tmp/rdd/realtime/")

    //TODO store the avg info in a same table
    val minAvgTblName = conf.getStringOrElse(JavaV.PERSIST_MONGODB_AVG_TBLNAME, "avgdata")

    // file backup store in a same palce
    val fileAvgBasePath = conf.getStringOrElse(JavaV.PERSIST_FILE_AVG_BASEPATH, "file:///tmp/rdd/avg/")

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
      val timeout = conf.getStringOrElse(JavaV.PERSIST_MONGODB_DBNAME, "1000")
      properties.put(JavaV.PERSIST_MONGODB_DBNAME, timeout)
      val hconf = new HConf(streamingContext.sparkContext.hadoopConfiguration)
      val persistenceSink = PersistenceSink(hconf, properties)
      streamingContext.sparkContext.broadcast(persistenceSink)
    }

    val kafkaStream = SimpleKafkaUtils.getStream(streamingContext, conf)

    val invalidAccumulator = streamingContext.sparkContext.accumulator(0)

    streamingContext.addStreamingListener(new StreamingListener {

      override def onBatchStarted(batchStarted: StreamingListenerBatchStarted): Unit = {
        val batchInfo = batchStarted.batchInfo
        log.info("***Batch started, " + batchInfo.numRecords)
      }

      override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
        val batchInfo = batchCompleted.batchInfo
        batchInfo.numRecords
        log.info("***Batch completed, " + batchInfo.batchTime + "SD: " + batchInfo.schedulingDelay.get
          + ", PD: " + batchInfo.processingDelay + ", TD: " + batchInfo.totalDelay
        )

        MessageMetric.invalidSet(invalidAccumulator.value)
        MessageMetric.validIncrease(batchInfo.numRecords)
        log.info(s"For now total ${MessageMetric.validNum} valid messages and ${MessageMetric.invalidNum} invalid message has been tackled")
      }
    })

    kafkaStream.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        val beginTime = System.currentTimeMillis()
        if (iter.hasNext) {
          val producer = kafkaProducerBroadcast.value
          //log.info("------------B producer------------")
          val avgCacheManager = avgCacheBroadcast.value
          //log.info("------------B avgCacheManager------------")
          val persistence = persistenceBroadcast.value
          //log.info("------------B persistence------------")
          val configMap = deviceConfigBroadcast.value.configMap

          //log.info("------------B configMap------------")
          implicit val jsonToPd = (jo: JSONObject) => new PersistenceDataJsonWrap(jo)
          val realtimeObjs = new util.HashMap[String, util.List[PersistenceData]]
          val avgObjs = new util.HashMap[String, util.List[PersistenceData]]
          val realtimePersistOpt = new MongoPersistenceOpt(dbname, realtimeTblName, new FilePersistenceOpt(fileRealtimeBasePath, null))
          val avgPersistOpt = new MongoPersistenceOpt(dbname, minAvgTblName, new FilePersistenceOpt(fileAvgBasePath, null))

          while (iter.hasNext) {
            val msg = iter.next()._2
            val msgJson = JSONObject.fromObject(msg)
            // following keys must be contained in a message
            val app = msgJson.getString(JsonField.DeviceValue.APP)
            val id = msgJson.getString(JsonField.DeviceValue.ID)
            val portNum = msgJson.getInt(JsonField.DeviceValue.PORTNUM)
            val dTimeStamp = msgJson.getLong(JsonField.DeviceValue.DTIMESTAMP)
            val values = msgJson.getJSONArray(JsonField.DeviceValue.VALUES)
            val ptimeStamp = msgJson.getLong(JsonField.DeviceValue.PTIMESTAMP)
            var portIdx = 0
            var hasInvalid = false
            while (portIdx < portNum) {
              val valueJson = values.getJSONObject(portIdx)
              val value = valueJson.get(JsonField.DeviceValue.VALUE)
              val action = configMap.getActions(id, portIdx)
              if (action != null && value != null) {
                log.info("id: " + id + ", conf.action: " + action.getClass.getName + ", type: " + action.avgType())
                val transferedV = action.transferedV(Actions.objToDouble(value))
                valueJson.put(JsonField.DeviceValue.VALUE, transferedV)
                val avgs = action.avgType()
                if (avgs != null) {
                  avgs.foreach(avg => {
                    val cinfo = avgCacheManager.addData(id, portIdx, ptimeStamp, transferedV, avg)
                    if (cinfo != null) {
                      val avgObj = AvgPersistenceUtil.avgPersistenceObj(id, portIdx, avg.avgName(),
                        dTimeStamp, ptimeStamp, cinfo.sumData, cinfo.sumNum)

                      val appAvgList = avgObjs.get(app)
                      if (appAvgList != null) {
                        appAvgList.add(avgObj)
                      } else {
                        val newAppAvgList = new util.LinkedList[PersistenceData]()
                        newAppAvgList.add(avgObj)
                        avgObjs.put(app, newAppAvgList)
                      }
                      producer.send(avgDataTopic, avgObj.toString)
                    }
                  })
                }
              }
              if (valueJson.containsKey(JsonField.DeviceValue.VALID) && !valueJson.getBoolean(JsonField.DeviceValue.VALID)) {
                hasInvalid = true
              }
              //printer.println(valueJson.toString())
              portIdx += 1
            }

            //printer.close()
            val appRealtimeList = realtimeObjs.get(app)
            if (appRealtimeList != null) {
              appRealtimeList.add(msgJson)
            } else {
              val newAppRealtimeList = new util.LinkedList[PersistenceData]()
              newAppRealtimeList.add(msgJson)
              realtimeObjs.put(app, newAppRealtimeList)
            }
            producer.send(realtimeTopic, msgJson.toString())
            //printer.println(msgJsonStr)
            //deviceConfigBroadcast.destroy()
            log.info("---------------------------count += 1")
            if (hasInvalid) {
              invalidAccumulator += 1
            }
          }
          log.info("---------------------AVG realtime----------------" + realtimeObjs.size() + realtimePersistOpt.getStr1 + realtimePersistOpt.getStr2)
          val realIter = realtimeObjs.entrySet().iterator()
          while (realIter.hasNext) {
            val entry = realIter.next()
            persistence.batch(entry.getValue, new MongoPersistenceOpt(entry.getKey, realtimeTblName, new FilePersistenceOpt(fileRealtimeBasePath, null)))
          }
          //persistence.batch(realtimeObjs, realtimePersistOpt, PersistenceLevel.BOTH)
          log.info("---------------------AVG avgdate----------------" + avgObjs.size() + avgPersistOpt.getStr1 + avgPersistOpt.getStr2)
          val avgIter = avgObjs.entrySet().iterator()
          while (realIter.hasNext) {
            val entry = avgIter.next()
            persistence.batch(entry.getValue, new MongoPersistenceOpt(entry.getKey, minAvgTblName, new FilePersistenceOpt(fileAvgBasePath, null)))
          }
          //persistence.batch(avgObjs, avgPersistOpt)
          //printer.close()
        } else {
          log.info("Received message is empty: " + System.currentTimeMillis())
        }
        log.info("Partition finished cost: " + (System.currentTimeMillis() - beginTime))
      })
      //rdd.saveAsTextFile("/tmp/rdd/%s".format(System.currentTimeMillis()))
    })
    streamingContext.start()
    streamingContext.awaitTermination()
    jobSqlManager.stop()
  }
}
