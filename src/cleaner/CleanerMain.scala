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
import conf.DECacheManagerSink
import conf.deviceconfig.action.{Actions}
import avgcache.{AvgFactory, AvgCacheManager}
import kafka.{SimpleKafkaUtils}
import metric._
import net.sf.json.JSONObject
import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.streaming.scheduler.{StreamingListenerBatchStarted, StreamingListenerBatchCompleted, StreamingListenerReceiverStarted, StreamingListener}
import org.apache.spark.{SparkContext, Logging, SparkConf}

import org.apache.spark.streaming.{Seconds, StreamingContext}
import persistence.PersistenceSink
import rpc.RPCServer
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
    val deployUrl = if (conf.getStringOrElse(JavaV.MASTER_HOST) == null) {
        "local[4]"
    } else {
      "spark://%s:7077".format(conf.getStringOrElse(JavaV.MASTER_HOST))
    }
    log.info("Spark deploy url is: " + deployUrl)
    val secInterval = conf.getLongOrElse(JavaV.STREAMING_TIME_SEC_INTERVAL)
    val sparkConf = new SparkConf()
    sparkConf.setAppName("Device Data Cleaner")
      .setMaster(deployUrl)
      // we modify spark rdd textFile method
      // if this parameter is false and there's
      // no file existing then EmptyRDD will be
      // returned instead of throwing an exception.
      // However, if this parameter is true and there's
      // no file existing then an exception is throwing directly
      .set("spark.throw.exception.without.resource", "false")
      // Can this make a difference ???
      .set("spark.streaming.unpersist", "true")
    val sparkContext = new SparkContext(sparkConf)

    val streamingContext = new StreamingContext(sparkContext, Seconds(secInterval))

    val rpcServer = new RPCServer()
    rpcServer.start(streamingContext.sparkContext, conf)

    val realtimeTopic = conf.getStringOrElse(JavaV.SPARKSTREAMING_CLEANED_TOPIC)
    val avgDataTopic = conf.getStringOrElse(JavaV.SPARKSTREAMING_AVG_TOPIC)
    val kafkaProducerBroadcast = {
      val kafkaSink = SimpleKafkaUtils.getKafkaSink(conf)
      streamingContext.sparkContext.broadcast(kafkaSink)
    }

    val avgCacheBroadcast = {
      streamingContext.sparkContext.broadcast(
      AvgCacheManager())
    }

    val dbname = conf.getStringOrElse(JavaV.PERSIST_MONGODB_DBNAME)
    val realtimeTblName = conf.getStringOrElse(JavaV.PERSIST_MONGODB_REALTIME_TBLNAME)

    // file backup store in a same palce
    val fileRealtimeBasePath = conf.getStringOrElse(JavaV.PERSIST_FILE_REALTIME_BASEPATH)

    //TODO store the avg info in a same table
    val minAvgTblName = conf.getStringOrElse(JavaV.PERSIST_MONGODB_AVG_TBLNAME)

    // file backup store in a same palce
    val fileAvgBasePath = conf.getStringOrElse(JavaV.PERSIST_FILE_AVG_BASEPATH)

    val deviceConfigBroadcast = {
      val properties = new Properties()
      streamingContext.sparkContext.broadcast(DECacheManagerSink(properties))
    }

    val persistenceBroadcast = {
      val properties = new Properties()
      properties.put(JavaV.PERSIST_MONGODB_HOST, conf.getStringOrElse(JavaV.PERSIST_MONGODB_HOST))
      properties.put(JavaV.PERSIST_MONGODB_PORT, conf.getStringOrElse(JavaV.PERSIST_MONGODB_PORT))
      properties.put(JavaV.PERSIST_MONGODB_DBNAME, dbname)
      properties.put(JavaV.PERSIST_MONGODB_DBNAME, conf.getStringOrElse(JavaV.PERSIST_MONGODB_TIMEOUT))
      val hconf = new HConf(streamingContext.sparkContext.hadoopConfiguration)
      val persistenceSink = PersistenceSink(hconf, properties)
      streamingContext.sparkContext.broadcast(persistenceSink)
    }

    val kafkaStream = SimpleKafkaUtils.getStream(streamingContext, conf)

    val invalidAccumulator = streamingContext.sparkContext.accumulator(0)


    val tenantAccumulator = streamingContext.sparkContext.
      accumulable(new util.ArrayList[AppAccumulatorInfo]().asInstanceOf[util.List[AppAccumulatorInfo]],
        "TENANT-ACCUMULATOR")(new AppAccumulator)

    val appAccumulator = streamingContext.sparkContext.
      accumulable(new util.ArrayList[AppAccumulatorInfo]().asInstanceOf[util.List[AppAccumulatorInfo]],
        "APP-ACCUMULATOR")(new AppAccumulator)

    val sceneAccumulator = streamingContext.sparkContext.
      accumulable(new util.ArrayList[AppAccumulatorInfo]().asInstanceOf[util.List[AppAccumulatorInfo]],
        "SCENE-ACCUMULATOR")(new AppAccumulator)

    val pointerAccumulator = streamingContext.sparkContext.
      accumulable(new util.ArrayList[AppAccumulatorInfo]().asInstanceOf[util.List[AppAccumulatorInfo]],
        "POINTER-ACCUMULATOR")(new AppAccumulator)

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
//        val appValues = appAccumulator.value
//        val iitt = appValues.iterator()
//        while (iitt.hasNext) {
//          val e = iitt.next()
//          log.info("--------" + e.id + " : " + e.mn+ " : " + e.mb)
//        }
        MessageMetric.tenantInfoIn(tenantAccumulator.value)
        MessageMetric.appInfoIn(appAccumulator.value)
        MessageMetric.sceneInfoIn(sceneAccumulator.value)
        MessageMetric.pointerInfoIn(pointerAccumulator.value)

        MessageMetric.showInfos(0)
        MessageMetric.showInfos(1)
        MessageMetric.showInfos(2)
        MessageMetric.showInfos(3)
      }
    })

    kafkaStream.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        val beginTime = System.currentTimeMillis()
        if (iter.hasNext) {
          // api layer producer
          val producer = kafkaProducerBroadcast.value
          // avg data cache manager
          val avgCacheManager = avgCacheBroadcast.value
          // data info persistence
          val persistence = persistenceBroadcast.value
          // device config map

          implicit val jsonToPd = (jo: JSONObject) => new PersistenceDataJsonWrap(jo)
          val realtimeObjs = new util.HashMap[String, util.List[PersistenceData]]
          val avgObjs = new util.HashMap[String, util.List[PersistenceData]]

          val realtimePersistOpt = new MongoPersistenceOpt(dbname, realtimeTblName, new FilePersistenceOpt(fileRealtimeBasePath, null))
          val avgPersistOpt = new MongoPersistenceOpt(dbname, minAvgTblName, new FilePersistenceOpt(fileAvgBasePath, null))

          val tenantM = new util.HashMap[Int, AppAccumulatorInfo]()
          val appM = new util.HashMap[Int, AppAccumulatorInfo]()
          val sceneM = new util.HashMap[Int, AppAccumulatorInfo]()
          val pointerM = new util.HashMap[Int, AppAccumulatorInfo]()

          while (iter.hasNext) {
            val msg = iter.next()._2
            val msgJson = JSONObject.fromObject(msg)
            // following keys must be contained in a message
            //val app = msgJson.getString(JsonField.DeviceValue.APP)
            val id = msgJson.getString(JsonField.DeviceValue.ID)
            //val portNum = msgJson.getInt(JsonField.DeviceValue.PORTNUM)
            //val dTimeStamp = msgJson.getLong(JsonField.DeviceValue.DTIMESTAMP)
            //val values = msgJson.getJSONArray(JsonField.DeviceValue.VALUES)
            val ptimeStamp = msgJson.getLong(JsonField.DeviceValue.PTIMESTAMP)
            val configMap = deviceConfigBroadcast.value
            //log.info("-----APPNAME: " + app)
            val des = if (configMap == null) null else configMap.actionByDcode(id)
            //val joAry = new Array[JSONObject](des.size())
            var idx = 0
            while (idx < des.size()) {
              val dei = des.get(idx)
              if (dei != null) {
                val joTmp = new JSONObject()
                joTmp.put("dcode", id)
                joTmp.put("uid", dei.getUid)
                joTmp.put("aid", dei.getAid)
                joTmp.put("sid", dei.getSid)
                joTmp.put("pid", dei.getPid)
                joTmp.put("unit", dei.getPoutunit)
                joTmp.put("st", ptimeStamp)
                val expr = dei.getExpr
                if (expr != null) {
                  val value = expr.compute(msgJson)
                  joTmp.put("value", value)
                  val avg = dei.getAvgs
                  if (avg.isDefined) {
                    avg.get.foreach(avg => {
                      val cinfo = avgCacheManager.avgCache.putData(dei.getPid.toString, ptimeStamp, value, avg)
                      if (cinfo != null) {
                        val avgObj = AvgPersistenceUtil.avgPersistenceObj(
                          id,
                          dei.getUid,
                          dei.getAid,
                          dei.getSid,
                          dei.getPid,
                          dei.getPoutunit,
                          cinfo.beginTime,
                          avg.avgName(),
                          cinfo.sumData,
                          cinfo.sumNum)
                        val appAvgList = avgObjs.get(dei.getAid.toString)
                        if (appAvgList != null) {
                          appAvgList.add(avgObj)
                        } else {
                          val newAppAvgList = new util.LinkedList[PersistenceData]()
                          newAppAvgList.add(avgObj)
                          avgObjs.put(dei.getAid.toString, newAppAvgList)
                        }
                        log.info("Push to Kafka at topic: %s, mes: %s".format(avgDataTopic, avgObj.toString()))
                        producer.send(avgDataTopic, avgObj.toString)
                      }
                    })
                  }
                }
                val appRealtimeList = realtimeObjs.get(dei.getAid.toString)
                if (appRealtimeList != null) {
                  appRealtimeList.add(msgJson)
                } else {
                  val newAppRealtimeList = new util.LinkedList[PersistenceData]()
                  newAppRealtimeList.add(msgJson)
                  realtimeObjs.put(dei.getAid.toString, newAppRealtimeList)
                }
                val joTmpStr = joTmp.toString()
                producer.send(realtimeTopic, joTmpStr)
                val bytes = joTmpStr.length

                // tenant statistic
                val tenantMetric = tenantM.get(dei.getUid)
                if (tenantMetric == null) {
                  tenantM.put(dei.getUid, AppAccumulatorInfo(dei.getUid, 1, bytes))
                } else {
                  tenantMetric.addWithDI((dei.getUid, 1, bytes))
                }

                // app statistic
                val appMetric = appM.get(dei.getAid)
                if (appMetric == null) {
                  appM.put(dei.getAid, AppAccumulatorInfo(dei.getAid, 1, bytes))
                } else {
                  appMetric.addWithDI((dei.getAid, 1, bytes))
                }

                // scene statistic
                val sceneMetric = sceneM.get(dei.getSid)
                if (sceneMetric == null) {
                  sceneM.put(dei.getSid, AppAccumulatorInfo(dei.getSid, 1, bytes))
                } else {
                  sceneMetric.addWithDI((dei.getSid, 1, bytes))
                }

                // pointer statistic
                val pointerMetric = pointerM.get(dei.getPid)
                if (pointerMetric == null) {
                  pointerM.put(dei.getPid, AppAccumulatorInfo(dei.getPid, 1, bytes))
                } else {
                  pointerMetric.addWithDI((dei.getPid, 1, bytes))
                }

                idx += 1
              }
            }
          }


          val tenantMetricList = new util.ArrayList[AppAccumulatorInfo](tenantM.size())
          val mmmiter0 = tenantM.entrySet().iterator()
          var idx = 0
          while (mmmiter0.hasNext) {
            val entry = mmmiter0.next()
            log.info("app-----------" + entry.getKey + " : " + entry.getValue)
            tenantMetricList.add(entry.getValue)
            idx += 1
          }
          tenantAccumulator += tenantMetricList


          val appMetricList = new util.ArrayList[AppAccumulatorInfo](appM.size())
          val mmmiter1 = appM.entrySet().iterator()
          while (mmmiter1.hasNext) {
            val entry = mmmiter1.next()
            log.info("app-----------" + entry.getKey + " : " + entry.getValue)
            appMetricList.add(entry.getValue)
            idx += 1
          }
          appAccumulator += appMetricList


          val sceneMetricList = new util.ArrayList[AppAccumulatorInfo](sceneM.size())
          val mmmiter2 = sceneM.entrySet().iterator()
          while (mmmiter2.hasNext) {
            val entry = mmmiter2.next()
            log.info("app-----------" + entry.getKey + " : " + entry.getValue)
            sceneMetricList.add(entry.getValue)
            idx += 1
          }
          sceneAccumulator += sceneMetricList

          val pointerMetricList = new util.ArrayList[AppAccumulatorInfo](pointerM.size())
          val mmmiter3 = pointerM.entrySet().iterator()
          while (mmmiter3.hasNext) {
            val entry = mmmiter3.next()
            log.info("app-----------" + entry.getKey + " : " + entry.getValue)
            pointerMetricList.add(entry.getValue)
            idx += 1
          }
          pointerAccumulator += pointerMetricList

          log.info("---------------------AVG realtime----------------" + realtimeObjs.size() + realtimePersistOpt.getStr1 + realtimePersistOpt.getStr2)
          val realIter = realtimeObjs.entrySet().iterator()
          while (realIter.hasNext) {
            val entry = realIter.next()
            persistence.batch(entry.getValue, new MongoPersistenceOpt("app" + entry.getKey, realtimeTblName, new FilePersistenceOpt(fileRealtimeBasePath, null)))
          }
          log.info("---------------------AVG avgdate----------------" + avgObjs.size() + avgPersistOpt.getStr1 + avgPersistOpt.getStr2)
          val avgIter = avgObjs.entrySet().iterator()
          while (avgIter.hasNext) {
            val entry = avgIter.next()
            persistence.batch(entry.getValue, new MongoPersistenceOpt("app" + entry.getKey, minAvgTblName, new FilePersistenceOpt(fileAvgBasePath, null)))
          }
        } else {
          log.info("Received message is empty: " + System.currentTimeMillis())
        }
        log.info("Partition finished cost: " + (System.currentTimeMillis() - beginTime))
      })
    })
    streamingContext.start()
    streamingContext.awaitTermination()
    rpcServer.stop()
  }
}
