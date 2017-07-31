package conf

import java.util
import java.util.{UUID, Properties}
import java.util.concurrent.atomic.AtomicInteger
import javaclz.JavaV
import javaclz.mysql.MySQLAccessor
import javaclz.zk.ZkManager

import com.mchange.v2.c3p0.ComboPooledDataSource
import conf.action._
import conf.deviceconfig.{AppEntry, AppConfigMap, DeviceConfigMap}
import net.sf.json.JSONObject
import org.I0Itec.zkclient.{IZkChildListener, IZkDataListener}
import org.apache.spark.Logging
import org.apache.zookeeper.CreateMode

/**
  * Created by xiaoke on 17-6-4.
  */
trait NodeHook {
  def trigerOn(app: String, data: AnyRef)
}

object DeviceConfigMananger {

  private class NodeHookImp(val mark: String, p:(String, AnyRef) => Unit) extends NodeHook {

    override def trigerOn(app: String, data: AnyRef): Unit = p(app, data)

    override def hashCode(): Int = mark.hashCode()

    override def equals(obj: scala.Any): Boolean = {
      obj match {
        case other: NodeHookImp => mark.equals(other.mark)
        case _ => false
      }
    }
  }

  private val newAppNodeHook = new util.HashSet[NodeHook]

  private val deleteAppNodeHook = new util.HashSet[NodeHook]

  def trigerHook(app: String, data: AnyRef, isNew: Boolean) = {
     if (isNew) {
      newAppNodeHook.synchronized {
        val iter = newAppNodeHook.iterator()
        while (iter.hasNext) {
          val hook = iter.next()
          try {
            hook.trigerOn(app, data)
          } catch {
            case e: Throwable => //ignore
          }
        }
      }
    } else {
       deleteAppNodeHook.synchronized {
         val iter = deleteAppNodeHook.iterator()
         while (iter.hasNext) {
           val hook = iter.next()
           try {
             hook.trigerOn(app, data)
           } catch {
             case e: Throwable => //ignore
           }
         }
       }
    }
  }

  def addNewHook(nh: NodeHook): String = newAppNodeHook.synchronized {
    val newMark = UUID.randomUUID().toString
    newAppNodeHook.add(new NodeHookImp(newMark, nh.trigerOn))
    newMark
  }

  def addDeleteHook(nh: NodeHook): String = deleteAppNodeHook.synchronized {
    val newMark = UUID.randomUUID().toString
    deleteAppNodeHook.add(new NodeHookImp("xxx", nh.trigerOn))
    newMark
  }

  def rmNewHook(nh: String): Unit = newAppNodeHook.synchronized {
    newAppNodeHook.remove(new NodeHookImp(nh, null))
  }

  def rmDeleteHook(nh: String): Unit = deleteAppNodeHook.synchronized {
    deleteAppNodeHook.remove(new NodeHookImp(nh, null))
  }

}

class DeviceConfigMananger(prop: Properties) extends Logging{

  @transient private val curIndexes = new util.HashMap[String, AtomicInteger]()

  @transient private val deviceConfig: DeviceConfigMap = new DeviceConfigMap()

  @transient private val appConfig: AppConfigMap = new AppConfigMap()

  @transient private val childrenPathSet = new util.HashSet[String]()

  @transient @volatile var zkManagerForAppConf: ZkManager = _

  @transient @volatile var zkManagerForDeviceConf: ZkManager = _

  @transient @volatile var mysqlPool: ComboPooledDataSource = _

  init()

  def configMap = deviceConfig

  def getAppConf(appName: String) = appConfig.get(appName) match {
    case null => None
    case ae: AppEntry  => Some(ae)
  }

  private def init(): Unit = {
    initMySql()
    initZK()
    sys.addShutdownHook {
      if (mysqlPool != null) mysqlPool.close()
      if (zkManagerForAppConf != null) zkManagerForAppConf.close()
      if (zkManagerForDeviceConf != null) zkManagerForDeviceConf.close()
      if (appConfig != null) appConfig.clear()
      if (deviceConfig != null) deviceConfig.clear()
      if (curIndexes != null) curIndexes.clear()
    }
  }

  private def initMySql(): Unit = {
    val mysqlHost = prop.getProperty(JavaV.DC_MYSQL_HOST, "192.168.1.110")
    val mysqlPort = prop.getProperty(JavaV.DC_MYSQL_PORT, "3306").toInt
    val mysqlDbname = prop.getProperty(JavaV.DC_MYSQL_DBNAME, "device")
    val mysqlUser = prop.getProperty(JavaV.DC_MYSQL_USER, "root")
    val mysqlPasswd = prop.getProperty(JavaV.DC_MYSQL_PASSWD, "heheave")
    mysqlPool = MySQLAccessor.getDataSource(mysqlHost, mysqlPort, mysqlDbname, mysqlUser, mysqlPasswd)
  }

  private def tackleBasePathData(appBasePath: String, deviceBasePath: String, list: util.List[String]):
    Unit = zkManagerForAppConf.synchronized {
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
              val app = pathToApp(s)
              appConfig.remove(app)
              curIndexes.remove(app)
              DeviceConfigMananger.trigerHook(app, null, false)
              zkClientApp.unsubscribeDataChanges(s, this)
            }
          })
          val appData = zkClientApp.readData[String](addPathApp, true)
          tackleAppConfPathData(addPathApp, appData)


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
          val deviceData = zkClientDevice.readData[String](addPathDevice, true)
          tackleDeviceConfPathData(addPathDevice, deviceData)
        }
      } finally {
        toAdd.clear()
        newSet.clear()
      }
    }
  }


  private def tackleAppConfPathData(path: String, data: Any) = zkManagerForAppConf.synchronized {
    if (data != null) {
      try {
        val strData = data match {
          case str: String => str
          case _ => data.toString
        }
        val jo = JSONObject.fromObject(strData)
        log.info("-----" + jo.toString())
        DeviceConfigMananger.trigerHook(pathToApp(path), jo, true)
        //val idx = jo.getString("idx").toInt
        //loadDataById(idx)
      } catch {
        case e: Throwable => logWarning("Could not get id", e)
      }
    }
  }

  private def tackleDeviceConfPathData(path: String, data: Any) = zkManagerForAppConf.synchronized {
    if (data != null) {
      try {
        val strData = data match {
          case str: String => str
          case _ => data.toString
        }
        val jo = JSONObject.fromObject(strData)
        val idx = jo.getString("idx").toInt
        loadDataById(pathToApp(path), idx)
      } catch {
        case e: Throwable => logWarning("Could not get id", e)
      }
    }
  }

  private def initZK(): Unit = {
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
    val childPath = zkClientForAppConf.getChildren(appBasePath)
    tackleBasePathData(appBasePath, deviceBasePath, childPath)
//    if (childPath != null) {
//      val cpIter = childPath.iterator()
//      while (cpIter.hasNext) {
//        val cp = cpIter.next()
//        loadDataById(pathToApp(cp))
//      }
//    }
  }

  private def pathToApp(path: String): String = {
    if (path != null) {
      val lastIdx = path.lastIndexOf("/")
      if (lastIdx >= 0) {
        path.substring(lastIdx + 1)
      } else {
        path
      }
    } else {
      throw new NullPointerException("Null path cannot be convert to an app")
    }
  }

  private def getOrAddNewCurIndex(app: String): AtomicInteger = {
    val ci = curIndexes.get(app)
    if (ci != null) {
      ci
    } else {
      val newCi = new AtomicInteger(-1)
      curIndexes.put(app, newCi)
      newCi
    }
  }

  private def loadDataById(app: String, id: Int = 0) = {
    val curIndex = getOrAddNewCurIndex(app)
    val curId = curIndex.get()
    if (curId >= id) {
      val (did, pidx, action) = readBySingleId(app, id)
      if (did != null) {
        if (action != null) {
          deviceConfig.append(did, pidx, action)
        } else {
          deviceConfig.remove(did, pidx)
        }
      }
    } else {
      //curIndex.set(id)
      val trueId = curId + 1
      val res = readByAfterId(app, trueId)
      val iter = res.iterator()
      while (iter.hasNext) {
        val (did, pidx, action) = iter.next()
        deviceConfig.append(did, pidx, action)
      }
    }
  }

  private def readBySingleId(app: String, id: Int): (String, Int, Action) = {
    val appTbl = "dc_%s".format(app)
    val conn = mysqlPool.getConnection
    if (conn != null) {
      val pstat = conn.prepareStatement("select did, pidx, cmd, avg, used from " + appTbl + " where id = ?")
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

  private def readByAfterId(app: String, id: Int): util.List[(String, Int, Action)] = {
    val appTbl = "dc_%s".format(app)
    val conn = mysqlPool.getConnection
    if (conn != null) {
      val pstat = conn.prepareStatement("select id, did, pidx, cmd, avg, used from " + appTbl + " where id >= ? and used = 1")
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
        if (maxId >= 0) {
          val curIndex = getOrAddNewCurIndex(app)
          curIndex.set(maxId)
        }
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
      case "EXPR1" => Expr1Action(lines(1), avg = at)
      case _ => {
        log.warn("Unsupported conf.action: %s".format(cmd))
        null
      }
    }
  }
}

class DeviceConfigManangerSink(fun: () => DeviceConfigMananger) extends Serializable{

  private lazy val deviceConfig = fun()

  def configMap = deviceConfig.configMap

  def appConfig(appName: String) = deviceConfig.getAppConf(appName)
}

object DeviceConfigManangerSink {
  def apply(prop: Properties): DeviceConfigManangerSink  = {
    val f = () => {
      new DeviceConfigMananger(prop)
    }
    new DeviceConfigManangerSink(f)
  }
}
