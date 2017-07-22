package deviceconfig

import java.util
import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger
import javaclz.mysql.MySQLAccessor
import javaclz.zk.{ZkManager, ZkClientSelf}
import javaclz.JavaV

import action._
import com.mchange.v2.c3p0.ComboPooledDataSource
import net.sf.json.JSONObject
import org.I0Itec.zkclient.{IZkChildListener, ZkClient, IZkDataListener}
import org.apache.spark.Logging
import org.apache.zookeeper.CreateMode

/**
  * Created by xiaoke on 17-6-4.
  */

class DeviceConfigMananger(prop: Properties) extends Logging{

  @transient private val curIndex = new AtomicInteger(-1)

  @transient private val config: DeviceConfigMap = new DeviceConfigMap()

  @transient private val childrenPathSet = new util.HashSet[String]()

  @transient @volatile var zkManagerForAppConf: ZkManager = _

  @transient @volatile var zkManagerForDeviceConf: ZkManager = _

  @transient @volatile var mysqlPool: ComboPooledDataSource = _

  init()

  def init(): Unit = {
    initMySql()
    initZK()
    sys.addShutdownHook {
      if (mysqlPool != null) mysqlPool.close()
      if (zkManagerForAppConf != null) zkManagerForAppConf.close()
      if (zkManagerForDeviceConf != null) zkManagerForDeviceConf.close()
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

  def tackleBasePathData(appBasePath: String, deviceBasePath: String, list: util.List[String]): Unit = {
    val zkClientApp = zkManagerForAppConf.getClient
    val zkClientDevice = zkManagerForDeviceConf.getClient
    if (list == null || list.isEmpty) return
    childrenPathSet.synchronized {
      val toAdd = new util.ArrayList[String]
      val newSet = new util.ArrayList[String]
      val tmpIter = list.iterator()
      while (tmpIter.hasNext) {
        val p = tmpIter.next()
        if (childrenPathSet.remove(p)) {
          newSet.add(p)
        } else {
          toAdd.add(p)
        }
      }
      childrenPathSet.clear()
      try {
        childrenPathSet.addAll(newSet)
        val toAddIter = toAdd.iterator()
        while (toAddIter.hasNext) {
          val tmpPath = toAddIter.next()
          val addPathApp = "%s/%s".format(appBasePath, tmpPath)

          zkClientApp.subscribeDataChanges(addPathApp, new IZkDataListener {

            override def handleDataChange(s: String, o: scala.Any): Unit = {
              log.info("App conf change: " + s + " : " + o.toString)
              tackleAppConfPathData(s, o)
            }

            override def handleDataDeleted(s: String): Unit = {
              log.info("App conf path delete: " + s)
              zkClientApp.unsubscribeDataChanges(s, this)
            }
          })
          log.info(addPathApp)
          zkClientApp.exists(addPathApp)


          val addPathDevice = "%s/%s".format(deviceBasePath, tmpPath)
          zkClientDevice.subscribeDataChanges(addPathDevice, new IZkDataListener {

            override def handleDataChange(s: String, o: scala.Any): Unit = {
              log.info("Device conf change: " + s + " : " + o.toString)
              tackleDeviceConfPathData(s, o)
            }

            override def handleDataDeleted(s: String): Unit = {
              log.info("Device conf path delete: " + s)
              zkClientDevice.unsubscribeDataChanges(s, this)
            }
          })

        }

      } finally {
        toAdd.clear()
        newSet.clear()
      }
    }
  }


  def tackleAppConfPathData(path:String, data: Any) = {
    if (data != null) {
      try {
        val strData = data match {
          case str: String => str
          case _ => data.toString
        }
        val jo = JSONObject.fromObject(strData)
        log.info("-----" + jo.toString())
        //val idx = jo.getString("idx").toInt
        //loadDataById(idx)
      } catch {
        case e: Throwable => logWarning("Could not get id", e)
      }
    }
  }

  def tackleDeviceConfPathData(path: String, data: Any) = {
    if (data != null) {
      try {
        val strData = data match {
          case str: String => str
          case _ => data.toString
        }
        val jo = JSONObject.fromObject(strData)
        val idx = jo.getString("idx").toInt
        loadDataById(idx)
      } catch {
        case e: Throwable => logWarning("Could not get id", e)
      }
    }
  }

  def initZK(): Unit = {
    val zkHost = prop.getProperty(JavaV.DC_ZK_HOST, "192.168.1.110:2181")
    val zkTimeout = prop.getProperty(JavaV.DC_ZK_TIMEOUT, "10000").toInt
    val appBasePath = prop.getProperty(JavaV.DC_ZK_APP_PATH, "/application")
    val deviceBasePath = prop.getProperty(JavaV.DC_ZK_DEVICE_PATH, "/deviceconf")
    zkManagerForAppConf = new ZkManager(zkHost, zkTimeout)
    zkManagerForDeviceConf = new ZkManager(zkHost, zkTimeout)

    val zkClientForAppConf = zkManagerForAppConf.getClient
    if (!zkClientForAppConf.exists(appBasePath)) {
      zkClientForAppConf.create(appBasePath, null, CreateMode.PERSISTENT)
    }
    zkClientForAppConf.subscribeChildChanges(appBasePath, new IZkChildListener() {
      override def handleChildChange(s: String, list: util.List[String]): Unit = {
        tackleBasePathData(appBasePath, deviceBasePath, list)
      }
    })
    val childListener = zkClientForAppConf.getChildren(appBasePath)
    tackleBasePathData(appBasePath, deviceBasePath, childListener)




//    zkClient = new ZkClientSelf(zkHost, zkPort, zkTimeout, new ZkReconnected {
//      override def reconnected(zk: ZooKeeper): Unit = {
//        zk.exists(zkWatchPath, new Watcher {
//          override def process(watchedEvent: WatchedEvent): Unit = {
//            val path = watchedEvent.getPath
//            log.info(s"-----RECZK------watch event type: ${watchedEvent.getType} at path ${path}")
//            if (watchedEvent.getType == Event.EventType.NodeCreated
//              || watchedEvent.getType == Event.EventType.NodeDataChanged) {
//              val info = zk.getData(path, false, null)
//              if (info != null) {
//                try {
//                  val id = new String(info).toInt
//                  log.info("-----RECZK------change at id: " + id)
//                  loadDataById(id)
//                } catch {
//                  case e: Throwable => log.warn("Could not get id", e)
//                }
//              }
//            }
//            zk.exists(zkWatchPath, this)
//          }
//        })
//      }
//    })
//
//    val zk = zkClient.zk
//    zk.exists(zkWatchPath, new Watcher {
//      override def process(watchedEvent: WatchedEvent): Unit = {
//        val path = watchedEvent.getPath
//        log.info(s"-----ZK------watch event type: ${watchedEvent.getType} at path ${path}")
//        if (watchedEvent.getType == Event.EventType.NodeCreated
//          || watchedEvent.getType == Event.EventType.NodeDataChanged) {
//          val info = zk.getData(path, false, null)
//          if (info != null) {
//            try {
//              val id = new String(info).toInt
//              log.info("-----ZK------change at id: " + id)
//              loadDataById(id)
//            } catch {
//              case e: Throwable => log.warn("Could not get id", e)
//            }
//          }
//        }
//        zk.exists(path, this)
//      }
// })
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
