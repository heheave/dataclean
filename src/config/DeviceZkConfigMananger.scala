package config

import java.util
import java.util.Properties
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger
import javaclz.mysql.MySQLAccessor
import javaclz.zk.ZkClient
import javaclz.zk.ZkClient.ZkReconnected
import javaclz.JavaV

import action._
import org.apache.log4j.Logger
import org.apache.zookeeper.Watcher.Event
import org.apache.zookeeper.{WatchedEvent, Watcher, ZooKeeper}

/**
  * Created by xiaoke on 17-6-4.
  */
class DeviceZkConfigMananger(prop: Properties) {

  private val log = Logger.getLogger(this.getClass)

  private val curIndex = new AtomicInteger(-1)

  private val config: Config = new Config()

  private val latch = new CountDownLatch(1)

  private val zkHost = prop.getProperty(JavaV.DC_ZK_HOST, "localhost")

  private val zkPort = prop.getProperty(JavaV.DC_ZK_PORT, "2181").toInt

  private val zkTimeout = prop.getProperty(JavaV.DC_ZK_TIMEOUT, "10000").toInt

  private val zkWatchPath = prop.getProperty(JavaV.DC_ZK_PATH, "/deviceConfig/change")

  private val mysqlHost = prop.getProperty(JavaV.DC_MYSQL_HOST, "localhost")

  private val mysqlPort = prop.getProperty(JavaV.DC_MYSQL_PORT, "3306").toInt

  private val mysqlDbname = prop.getProperty(JavaV.DC_MYSQL_DBNAME, "device")

  private val mysqlUser = prop.getProperty(JavaV.DC_MYSQL_USER, "root")

  private val mysqlPasswd = prop.getProperty(JavaV.DC_MYSQL_PASSWD, "heheave")

  init()

  def init(): Unit = {
    val zkClient = new ZkClient(zkHost, zkPort, zkTimeout, new ZkReconnected {
      override def reconnected(zk: ZooKeeper): Unit = {
        zk.exists(zkWatchPath, new Watcher {
          override def process(watchedEvent: WatchedEvent): Unit = {
            log.info("-----ZK------" + watchedEvent.getType)
            if (watchedEvent.getType == Event.EventType.NodeCreated
              || watchedEvent.getType == Event.EventType.NodeDataChanged) {
              val info = zk.getData(zkWatchPath, false, null)
              val id = new String(info).toInt
              log.info("-----ZK------change at id: " + id)
              loadDataById(id)
            }
            zk.exists(zkWatchPath, this)
          }
        })
      }
    })
    loadDataById(0)
    val zk = zkClient.zk
    zk.exists(zkWatchPath, new Watcher {
      override def process(watchedEvent: WatchedEvent): Unit = {
        log.info("-----ZK------" + watchedEvent.getType)
        if (watchedEvent.getType == Event.EventType.NodeCreated
          || watchedEvent.getType == Event.EventType.NodeDataChanged) {
          val info = zk.getData(zkWatchPath, false, null)
          val id = new String(info).toInt
          log.info("-----ZK------change at id: " + id)
          loadDataById(id)
        }
        zk.exists(zkWatchPath, this)
      }
    })

    sys.addShutdownHook {
      zk.close()
    }
  }

  def configMap = config


  private def loadDataById(id: Int) = {
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
      curIndex.set(id)
      val res = readByAfterId(id)
      val iter = res.iterator()
      while (iter.hasNext) {
        val (did, pidx, action) = iter.next()
        config.append(did, pidx, action)
      }
    }
  }

  private def readBySingleId(id: Int): (String, Int, Action) = {
    val conn = MySQLAccessor.getConnector(mysqlHost, mysqlPort, mysqlDbname, mysqlUser, mysqlPasswd)
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
    val conn = MySQLAccessor.getConnector(mysqlHost, mysqlPort, mysqlDbname, mysqlUser, mysqlPasswd)
    if (conn != null) {
      val pstat = conn.prepareStatement("select id, did, pidx, cmd, avg, used from deviceConfig where id >= ?")
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
            val used = resSet.getInt("used")
            val cmd = resSet.getString("cmd")
            if (used == 1 && cmd != null) {
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

  private def avgType(avg: String) = avg match {
    case "DAY" => Actions.AVG_DAY
    case "HOUR" => Actions.AVG_HOUR
    case "MIN" => Actions.AVG_MIN
    case _ => Actions.AVG_NONE
  }

  private def parseLine(cmd: String, avg: String) = {
    val at = avgType(avg)
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

class DeviceZkConfigManangerSink(fun: () => DeviceZkConfigMananger) extends Serializable{

  private lazy val config = fun()

  def configMap = config.configMap
}

object DeviceZkConfigManangerSink {
  def apply(prop: Properties): DeviceZkConfigManangerSink  = {
    val f = () => {
      new DeviceZkConfigMananger(prop)
    }
    new DeviceZkConfigManangerSink(f)
  }
}