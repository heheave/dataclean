package sql

import java.io._
import java.net.{Socket, InetSocketAddress, ServerSocket}
import java.text.SimpleDateFormat
import java.util
import java.util.Map.Entry
import java.util.{Comparator, Date, Properties, UUID}
import java.util.concurrent.{Executors, ConcurrentHashMap}
import java.util.concurrent.atomic.AtomicInteger
import javaclz.{JsonField, JavaV, JavaConfigure}

import net.sf.json.JSONObject
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{Logging, SparkEnv, SparkContext}
import org.bson.BSONObject

/**
  * Created by xiaoke on 17-6-2.
  */
class SqlJobMnager(sparkContext: SparkContext, jconf: JavaConfigure) extends Serializable with Logging{

  private val socketIdGen = new AtomicInteger(0)

  private val sparkSqlContext = new SQLContext(sparkContext)

  val socketServer = new ServerSocket()

  val socketMap = new ConcurrentHashMap[Int, Socket]()

  val executors = Executors.newFixedThreadPool(11)

  @volatile var isAlive: Boolean = _

  def start(): Unit ={
    isAlive = true
    val mainRunner = new Runnable {
      override def run(): Unit = {
        socketServer.bind(new InetSocketAddress("localhost", 20000))
        while (isAlive) {
          try {
            val newSocket = socketServer.accept()
            val socketId = socketIdGen.getAndIncrement()
            socketMap.put(socketId, newSocket)
            startNewJob(socketId)
          } catch {
            case ioe: IOException => log.warn("ServerSocket IOException ", ioe)
            case e: Exception => log.warn("ServerSocket Exception", e)
          }
        }
      }
    }
    executors.submit(mainRunner)
    log.info("SqlJobManager has been started")
  }


  def stop() = {
    isAlive = false
    executors.shutdown()
    socketServer.close()
    val socketMapIter = socketMap.entrySet().iterator()
    while (socketMapIter.hasNext) {
      retVal(Array("interpreted error"), socketMapIter.next().getKey)
    }
    log.info("SqlJobManager has stopped")
  }

  private def retVal(vals: Array[String], socketId: Int): Unit = {
    val socket = socketMap.get(socketId)
    try {
      if (socket != null && !socket.isClosed && !socket.isOutputShutdown) {
        val dos = new DataOutputStream(socket.getOutputStream)
        vals.foreach(v => {
          val bytes = v.getBytes()
          dos.write(bytes, 0, bytes.length)
        })
        dos.close()
      }
    } finally {
      socket.close()
    }
  }

  private def getHdfsRdd(time: Long) = {
    val dayStr = SqlJobMnager.hdfsDateFormat.format(time)
    val dataBasePath = jconf.getStringOrElse(JavaV.SPARK_SQL_HDFS_BASEPATH, "/tmp/rdd/realtime/")
    val dataPath = if (dataBasePath.endsWith("/")) {
      dataBasePath + "%s/*".format(dayStr)
    } else {
      dataBasePath + "/%s/*".format(dayStr)
    }
    val minPartition = jconf.getIntOrElse(JavaV.SPARK_SQL_MIN_PARTITION, -1)
    val textRdd = if(minPartition > 0) {
      sparkContext.textFile(dataPath, minPartition)
    } else {
      sparkContext.textFile(dataPath)
    }
    val hdfsRdd = textRdd.map(JSONObject.fromObject(_))
    hdfsRdd
  }

  private def getMongoRdd(time: Long) = {
    val dayStr = SqlJobMnager.mongoDateFormat.format(time)
    val configure = sparkContext.hadoopConfiguration
    val mongoInputUri = jconf.getStringOrElse(JavaV.SPARK_SQL_MONGO_BASEPATH, "mongodb://192.168.1.110:27017/device")
    configure.set("mongo.input.uri", mongoInputUri + ".realtime_%s".format(dayStr))
    configure.set("mongo.input.fields", SqlJobMnager.genMongoField())
    configure.set("mongo.input.noTimeout", "true")
    val mongoRdd = sparkContext.newAPIHadoopRDD(
      configure,
      classOf[com.mongodb.hadoop.MongoInputFormat],
      classOf[Object],
      classOf[BSONObject]
    ).map(doc => {
      val jo = JSONObject.fromObject(doc._2)
      jo.remove("_id")
      jo
    })
    mongoRdd
  }

  private def startNewJob(socketId: Int): Unit = {
    val socket = socketMap.get(socketId)
    val in = new BufferedReader(new InputStreamReader(socket.getInputStream))
    val line = in.readLine()
    log.info("--------------------------------------------" + line)
    log.info("--------------------------------------------" + line)
    log.info("--------------------------------------------" + line)

    if (line != null) {
      try {
        val json = JSONObject.fromObject(line)
        val jobType = json.remove(SqlCmdField.TYPE)
        var runner: Runnable = null
        if (jobType.equals(SqlCmdField.SQL)) {
          val s = json.get(SqlCmdField.QUERY).toString
          val p = if (json.containsKey(SqlCmdField.PORT)) json.getInt(SqlCmdField.PORT) else -1
          val dayTime = if (json.containsKey(SqlCmdField.DTIME)) json.getLong(SqlCmdField.DTIME) else System.currentTimeMillis()
          val mongoRdd = getMongoRdd(dayTime)
          val hdfsRdd = getHdfsRdd(dayTime)
          val unionRdd = mongoRdd.union(hdfsRdd)
          val needToReplaceTblName = jconf.getStringOrElse(JavaV.SPARK_SQL_TEMP_TBLNAME, "deviceValue")
          runner = new Runnable {
            override def run(): Unit = {
              val bt = System.currentTimeMillis()
              log.info(s"--------------------------------------------Task start at: ${bt}")
              val sqlRet = try {
                val jobTblName = "Table%s".format(UUID.randomUUID().toString.substring(0, 7))
                val realQuery = s.replaceAll(needToReplaceTblName, jobTblName)
                SqlJobMnager.jobSql(sparkSqlContext, unionRdd, jobTblName, realQuery, p)
              } catch {
                case e: Throwable => {
                  log.warn("Sql Error", e)
                  Array[String]()
                }
              }
              val et = System.currentTimeMillis()
              log.info(s"--------------------------------------------Task finished at: ${bt}, total time cost is: ${et - bt}")
              retVal(sqlRet, socketId)
            }
          }
        } else {
          val btime = if (json.containsKey(SqlCmdField.BTIME)) {
            json.remove(SqlCmdField.BTIME).asInstanceOf[Long]
          } else {
            -1L
          }

          log.info("-------------btime-----------" + btime)
          val etime = if (json.containsKey(SqlCmdField.ETIME)) {
            json.remove(SqlCmdField.ETIME).asInstanceOf[Long]
          } else {
            -1L
          }
          log.info("-------------etime-----------" + etime)
          val dayTime = if (btime > 0 && etime > 0) {
            val dayMillSec = 24 * 60 * 60 * 1000
            if (btime / dayMillSec == etime / dayMillSec) {
              btime
            } else {
              System.currentTimeMillis()
            }
          } else if (btime > 0) {
            btime
          } else if (etime > 0) {
            etime
          } else {
            System.currentTimeMillis()
          }
          val interval = if (json.containsKey(SqlCmdField.INTERVAL)) {
            json.remove(SqlCmdField.INTERVAL).asInstanceOf[Int]
          } else {
            -1
          }
          val delta = if (json.containsKey(SqlCmdField.DELTA)) {
            json.remove(SqlCmdField.DELTA).asInstanceOf[Int]
          } else {
            1000
          }
          log.info("-------------interval-----------" + interval)
          val prop = new Properties()
          val iter = json.entrySet.iterator
          while (iter.hasNext) {
            val entry = iter.next().asInstanceOf[Entry[Object, Object]]
            val key = entry.getKey
            val value = entry.getValue
            prop.put(key, value)
          }
          val mongoRdd = getMongoRdd(dayTime)
          val hdfsRdd = getHdfsRdd(dayTime)
          val unionRdd = mongoRdd.union(hdfsRdd)
          log.info("--------------------------------------------begin to run job")
          runner = new Runnable {
            override def run(): Unit = {
              val bt = System.currentTimeMillis()
              log.info(s"--------------------------------------------Task start at: ${bt}")
              val rddRet = try {
                SqlJobMnager.jobRdd(unionRdd, prop, btime, etime, interval, delta)
              } catch {
                case e: Throwable => {
                  log.warn("Rdd Error", e)
                  Array[String]()
                }
              }
              val et = System.currentTimeMillis()
              log.info(s"--------------------------------------------Task finished at: ${bt}, total time cost is: ${et - bt}")
              retVal(rddRet, socketId)
            }
          }
        }
        //      val runner = new Runnable {
        //        // it's the real sql job
        //        // be careful to spark closure problem
        //        override def run(): Unit = {
        //
        //          val (sql, pidx) = {
        //            val infos = line.split(":")
        //            var s: String = null
        //            var i = -1
        //            if (infos.length > 0) {
        //              s = infos(0).replaceAll(needToReplaceTblName, realTblName)
        //            }
        //            if (infos.length > 1) {
        //              i = try { infos(1).toInt } catch {
        //                case e: Throwable => {
        //                  retVal(Array(e.getMessage), socketId)
        //                  return
        //                }
        //              }
        //            }
        //            (s, i)
        //          }
        //          log.info("--------------------------------------------" + SparkEnv.get.executorId)
        //
        //
        //        }
        //      }
        if (runner != null) {
          executors.submit(runner)
        } else {
          retVal(Array("Null job occurred"), socketId)
        }
      } catch {
        case e: Throwable => {
          log.warn("--------Throwable-----------" + e)
          retVal(Array(e.getMessage), socketId)
        }
      }
    } else {
      retVal(Array("Null job info"), socketId)
    }
  }
}

case class  DeviceValue(did: String,
                        pidx: Int,
                        dtimestamp: Long,
                        ptimestamp: Long,
                        desc: String,
                        value: Double,
                        valid: Boolean)

object SqlJobMnager {

  val mongoDateFormat = new SimpleDateFormat("yyyyMMdd")

  val hdfsDateFormat = new SimpleDateFormat("yyyy-MM-dd")

  def jobSql(sqlContext: SQLContext, rdd: RDD[JSONObject], tblName: String, sqlStr: String, pidx: Int): Array[String] = {
    // generator table DeviceValue
    // DeviceValue(did: String, portNum: Int, dtimestamp: Long, ptimestamp: Long, desc: String, value: Double, valid: Boolean)
    import sqlContext.implicits._
    val deviceValue = rdd.map(jo => {
      val did = if (jo.containsKey(JsonField.DeviceValue.ID)) jo.getString(JsonField.DeviceValue.ID) else null
      val dtimestamp = if (jo.containsKey(JsonField.DeviceValue.DTIMESTAMP))
        jo.getLong(JsonField.DeviceValue.DTIMESTAMP) else 0
      val ptimestamp = if (jo.containsKey(JsonField.DeviceValue.PTIMESTAMP))
        jo.getLong(JsonField.DeviceValue.PTIMESTAMP) else 0
      val desc = if (jo.containsKey(JsonField.DeviceValue.DESC))
        jo.getString(JsonField.DeviceValue.DESC) else null
      val (value, valid) = if (jo.containsKey(JsonField.DeviceValue.VALUES)) {
        val ja = jo.getJSONArray(JsonField.DeviceValue.VALUES)
        if (pidx < 0 || pidx >= ja.size()) {
          (0.0, false)
        } else {
          val vinfo = ja.getJSONObject(pidx)
          val v = if (vinfo.containsKey(JsonField.DeviceValue.VALUE))
            vinfo.getDouble(JsonField.DeviceValue.VALUE) else 0.0
          val vl = if (vinfo.containsKey(JsonField.DeviceValue.VALID))
            vinfo.getBoolean(JsonField.DeviceValue.VALID) else false
          (v, vl)
        }
      } else {
        (0.0, false)
      }
      DeviceValue(did, pidx, dtimestamp, ptimestamp, desc, value, valid)
    }).toDF()
    deviceValue.registerTempTable(tblName)
    try {
      val result = sqlContext.sql(sqlStr).collect().map(row => {
        row.toString()
      })
      result
    } finally {
      sqlContext.dropTempTable(tblName)
    }
  }

  // for sample
  def jobRdd(rdd: RDD[JSONObject], prop: Properties, btime: Long, etime: Long, interval: Int, delta: Int): Array[String] = {
    val resultJOS = rdd.filter(jo => {
      filterJsonByProp(jo, prop, btime, etime, interval, delta)
    }).collect()
    util.Arrays.sort(resultJOS, new Comparator[JSONObject]{
      override def compare(o1: JSONObject, o2: JSONObject): Int = {
        (o1.getLong(JsonField.DeviceValue.PTIMESTAMP) -
          o2.getLong(JsonField.DeviceValue.PTIMESTAMP)).toInt
      }
    })
    resultJOS.map(_.toString())
  }

  private def filterJsonByProp(jo: JSONObject, prop: Properties, btime: Long, etime: Long, interval: Int, delta: Int): Boolean = {
    var result = true
    result &= {
      val ptime = if (jo.containsKey(JsonField.DeviceValue.PTIMESTAMP)) {
        jo.getLong(JsonField.DeviceValue.PTIMESTAMP)
      } else {
        -1L
      }

      if (ptime == -1) {
        if (btime > 0 || etime > 0 || interval > 0) {
          false
        } else {
          true
        }
      } else {
        val b = if (btime > 0) {
          if (btime <= ptime) {
            true
          } else {
            false
          }
        } else {
          true
        }

        val e = if (b) {
          if (etime > 0) {
            if (etime >= ptime) {
              true
            } else {
              false
            }
          } else {
            true
          }
        } else {
          false
        }

        val i = if (e) {
          if (interval > 0) {
            if (ptime % interval <= delta) {
              true
            } else {
              false
            }
          } else {
            true
          }
        } else {
          false
        }
       i
      }
    }

    val iter = prop.entrySet().iterator()
    while (result && iter.hasNext) {
      val entry = iter.next()
      val key = entry.getKey
      val value = entry.getValue
      result &= (jo.containsKey(key) && jo.get(key).equals(value))
    }
    result
  }

  def genMongoField(): String = {
    val fieldSeq = Seq(
      JsonField.DeviceValue.ID,
      JsonField.DeviceValue.PORTNUM,
      JsonField.DeviceValue.MTYPE,
      JsonField.DeviceValue.DTYPE,
      JsonField.DeviceValue.PTIMESTAMP,
      JsonField.DeviceValue.DTIMESTAMP,
      JsonField.DeviceValue.VALUES,
      JsonField.DeviceValue.DESC
    )
    val sb = new StringBuilder
    sb.append('{')
    var idx = 0
    while(idx < fieldSeq.size) {
      sb.append('\"')
      sb.append(fieldSeq(idx))
      sb.append("\":1")
      if (idx != fieldSeq.size - 1) {
        sb.append(',')
      } else {
        sb.append('}')
      }
      idx += 1
    }
    sb.toString()
  }
}