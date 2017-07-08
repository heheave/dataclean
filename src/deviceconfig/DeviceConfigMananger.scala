package deviceconfig

import java.util
import java.util.Properties
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger
import javaclz.mysql.MySQLAccessor
import javaclz.zk.ZkClient
import javaclz.zk.ZkClient.ZkReconnected
import javaclz.JavaV

import action._
import com.mchange.v2.c3p0.ComboPooledDataSource
import org.apache.log4j.Logger
import org.apache.spark.Logging
import org.apache.zookeeper.Watcher.Event
import org.apache.zookeeper.{WatchedEvent, Watcher, ZooKeeper}

/**
  * Created by xiaoke on 17-6-4.
  */
class DeviceConfigMananger(prop: Properties) extends Logging{

  @transient private val curIndex = new AtomicInteger(-1)

  @transient private val config: DeviceConfigMap = new DeviceConfigMap()

  @transient @volatile var zkClient: ZkClient = _

  @transient @volatile var mysqlPool: ComboPooledDataSource = _

  init()

  def init(): Unit = {
    initMySql()
    initZK()
    sys.addShutdownHook {
      if (mysqlPool != null) mysqlPool.close()
      if (zkClient != null) zkClient.close()
      if (config != null) config.clear()
    }
  }

  def initMySql(): Unit = {
    val mysqlHost = prop.getProperty(JavaV.DC_MYSQL_HOST, "192.168.1.110")
    val mysqlPort = prop.getProperty(JavaV.DC_MYSQL_PORT, "3306").toInt
    val mysqlDbname = prop.getProperty(JavaV.DC_MYSQL_DBNAME, "device")
    val mysqlUser = prop.getProperty(JavaV.DC_MYSQL_USER, "root")
    val mysqlPasswd = prop.getProperty(JavaV.DC_MYSQL_PASSWD, "heheave")
    mysqlPool = MySQLAccessor.getDataSource(mysqlHost, mysqlPort, mysqlDbname, mysqlUser, mysqlPasswd)
    loadDataById()
  }

  def initZK(): Unit = {
    val zkHost = prop.getProperty(JavaV.DC_ZK_HOST, "192.168.1.110")
    val zkPort = prop.getProperty(JavaV.DC_ZK_PORT, "2181").toInt
    val zkTimeout = prop.getProperty(JavaV.DC_ZK_TIMEOUT, "10000").toInt
    val zkWatchPath = prop.getProperty(JavaV.DC_ZK_PATH, "/deviceConfig/change")
    zkClient = new ZkClient(zkHost, zkPort, zkTimeout, new ZkReconnected {
      override def reconnected(zk: ZooKeeper): Unit = {
        zk.exists(zkWatchPath, new Watcher {
          override def process(watchedEvent: WatchedEvent): Unit = {
            val path = watchedEvent.getPath
            log.info(s"-----RECZK------watch event type: ${watchedEvent.getType} at path ${path}")
            if (watchedEvent.getType == Event.EventType.NodeCreated
              || watchedEvent.getType == Event.EventType.NodeDataChanged) {
              val info = zk.getData(path, false, null)
              if (info != null) {
                try {
                  val id = new String(info).toInt
                  log.info("-----RECZK------change at id: " + id)
                  loadDataById(id)
                } catch {
                  case e: Throwable => log.warn("Could not get id", e)
                }
              }
            }
            zk.exists(zkWatchPath, this)
          }
        })
      }
    })

    val zk = zkClient.zk
    zk.exists(zkWatchPath, new Watcher {
      override def process(watchedEvent: WatchedEvent): Unit = {
        val path = watchedEvent.getPath
        log.info(s"-----ZK------watch event type: ${watchedEvent.getType} at path ${path}")
        if (watchedEvent.getType == Event.EventType.NodeCreated
          || watchedEvent.getType == Event.EventType.NodeDataChanged) {
          val info = zk.getData(path, false, null)
          if (info != null) {
            try {
              val id = new String(info).toInt
              log.info("-----ZK------change at id: " + id)
              loadDataById(id)
            } catch {
              case e: Throwable => log.warn("Could not get id", e)
            }
          }
        }
        zk.exists(path, this)
      }
    })
  }

  def configMap = config


  private def loadDataById(id: Int = 0) = {
    val curId = curIndex.get()
    if (curId >= id) {
      val (did, pidx, action) = readBySingleId(id)
      if (did != null) {
        if (action != null) {
          config.append(did, pidx, action)
        } else {
          config.remove(did, pidx)
        }
      }
    } else {
      //curIndex.set(id)
      val trueId = curId + 1
      val res = readByAfterId(trueId)
      val iter = res.iterator()
      while (iter.hasNext) {
        val (did, pidx, action) = iter.next()
        config.append(did, pidx, action)
      }
    }
  }

  private def readBySingleId(id: Int): (String, Int, Action) = {
    val conn = mysqlPool.getConnection
    if (conn != null) {
      val pstat = conn.prepareStatement("select did, pidx, cmd, avg, used from deviceConfig where id = ?")
      pstat.setInt(1, id)
      try {
        val resSet = pstat.executeQuery()
        if (resSet.next()) {
          val did = resSet.getString("did")
          val pidx = resSet.getInt("pidx")
          if (did != null) {
            val used = resSet.getInt("used")
            val cmd = resSet.getString("cmd")
            if (used == 1 && cmd != null) {
              val avg = resSet.getString("avg")
              val action = parseLine(cmd, avg)
              (did, pidx, action)
            } else {
              (did, pidx, null)
            }
          } else {
            (null, -1, null)
          }
        } else {
          (null, -1, null)
        }
      } finally {
        MySQLAccessor.closeConn(conn, pstat)
      }
    } else {
      (null, -1, null)
    }
  }

  private def readByAfterId(id: Int): util.List[(String, Int, Action)] = {
    val conn = mysqlPool.getConnection
    if (conn != null) {
      val pstat = conn.prepareStatement("select id, did, pidx, cmd, avg, used from deviceConfig where id >= ? and used = 1")
      pstat.setInt(1, id)
      try {
        var maxId = -1
        val resSet = pstat.executeQuery()
        val res = new util.ArrayList[(String, Int, Action)]()
        while (resSet.next()) {
          val id = resSet.getInt("id")
          if (id > maxId) maxId = id
          val did = resSet.getString("did")
          val pidx = resSet.getInt("pidx")
          if (did != null) {
            val cmd = resSet.getString("cmd")
            if (cmd != null) {
              val avg = resSet.getString("avg")
              val action = parseLine(cmd, avg)
              res.add(did, pidx, action)
            }
          }
        }
        if (maxId >= 0) curIndex.set(maxId)
        res
      } finally {
        MySQLAccessor.closeConn(conn, pstat)
      }
    } else {
      null
    }
  }


  private def parseLine(cmd: String, avg: String) = {
    val at = Actions.getActions(avg)
    val lines = cmd.split(":")
    lines(0) match {
      case "NEG" => NegAction(avg = at)
      case "ADD" => AddAction(lines(1).toDouble, avg = at)
      case "SUB" => SubAction(lines(1).toDouble, avg = at)
      case "MUL" => MulAction(lines(1).toDouble, avg = at)
      case "DIV" => DivAction(lines(1).toDouble, avg = at)
      case "RVS" => RvsAction(avg = at)
      case _ => {
        log.warn("Unsupported action: %s".format(cmd))
        null
      }
    }
  }
}

class DeviceConfigManangerSink(fun: () => DeviceConfigMananger) extends Serializable{

  private lazy val config = fun()

  def configMap = config.configMap
}

object DeviceConfigManangerSink {
  def apply(prop: Properties): DeviceConfigManangerSink  = {
    val f = () => {
      new DeviceConfigMananger(prop)
    }
    new DeviceConfigManangerSink(f)
  }
}
