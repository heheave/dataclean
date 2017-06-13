package test1

import java.io.IOException
import java.util.Properties
import java.util.concurrent.CountDownLatch
import javaclz.JavaV
import javaclz.mysql.MySQLAccessor
import javaclz.persist.data.PersistenceDataJsonWrap
import javaclz.persist.opt.MongoPersistenceOpt
import javaclz.zk.ZkClient

import deviceconfig.DeviceConfigMananger
import net.sf.json.JSONObject
import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.zookeeper.Watcher.Event.{EventType, KeeperState}
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper._
import org.apache.zookeeper.data.Stat

/**
  * Created by xiaoke on 17-6-1.
  */
object TestMain {

  private lazy val log = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    PropertyConfigurator.configure(JavaV.LOG_PATH)
//    val dbname = "device"
//    val tblname = "realtime"
//    val mongoPersistence = new MongoPersistenceOpt(dbname, tblname)
//
//    val properties = new Properties()
//    properties.put(JavaV.MONGODB_HOST, "localhost")
//    properties.put(JavaV.MONGODB_PORT, "27017")
//    properties.put(JavaV.MONGODB_DBNAME, dbname)
//    val persistenceSink = PersistenceSink(properties)
//    val jo = new JSONObject()
//    jo.put("heheh", "hahahaha")
//    persistenceSink.once(new PersistenceDataJsonWrap(jo), mongoPersistence)
//    val latch = new CountDownLatch(1)
//    val zk = new ZooKeeper("localhost:2181", 1000, new Watcher() {
//      override def process(watchedEvent: WatchedEvent): Unit = {
//        if (watchedEvent.getState == KeeperState.SyncConnected) {
//          latch.countDown()
//        }
//      }
//    })
//    latch.await()
//
//    val watch = new Watcher {
//      override def process(watchedEvent: WatchedEvent): Unit = {
//        if (watchedEvent.getType == EventType.NodeChildrenChanged) {
//          val path = watchedEvent.getPath
//          println(path)
//          println("-----------")
//          handleChildrenChanged(zk, path)
//        }
//      }
//    }
//
//    def handleChildrenChanged(zk: ZooKeeper, path: String): Unit = {
//      val list = zk.getChildren(path, watch)
//      println(list.size())
//      val iter = list.iterator()
//      while(iter.hasNext) {
//        val childPath = iter.next()
//        println(childPath)
//        println(new String(zk.getData(path + "/" + childPath, false, null)))
//        zk.delete(path + "/" + childPath, -1)
//      }
//    }
//
//    println("zk server inited")
////    try {
////      zk.create("/deviceConfig", "deviceConfig".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
////    } catch {
////      case ioe: Exception => ioe.printStackTrace()
////    }
//    //zk.delete("/deviceConfig/change", -1)
//
//
//    val list = zk.getChildren("/deviceConfig", watch)
////    val iter = list.iterator()
////    while(iter.hasNext) {
////      val childPath = iter.next()
////      println(childPath)
////      //zk.delete(path + "/" + childPath, -1)
////    }
//
//    zk.create("/deviceConfig/change", "deviceConfig".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL)
//
//    Thread.sleep(1000)
//    zk.create("/deviceConfig/change", "avc".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL)
////    zk.getChildren("/deviceConfig/change", new Watcher() {
////      override def process(watchedEvent: WatchedEvent): Unit = {
////        println(watchedEvent.toString)
////        if (watchedEvent.getType == EventType.NodeChildrenChanged) {
////          println(watchedEvent)
////          //zk.delete("/deviceConfig" + "/" + watchedEvent.getPath, -1)
////        } else if (watchedEvent.getType == EventType.NodeDeleted) {
////        }
////      }
////    })
//
////    zk.delete("/deviceConfig/ABC007", -1)
////    //zk.setData("/deviceConfig/ABC00200", "sdafsadfds".getBytes(), -1)
////    val iter = list.iterator()
////    while (iter.hasNext) {
////      val path = iter.next()
////      val data = zk.getData("/deviceConfig/" + path, false, null)
////      println(path + ":" + new String(data))
////    }


//    val deviceZkConfigManager = new DeviceConfigMananger(null)
//    deviceZkConfigManager.init()
//

//////
//////    var i = 0
//////    log.info("begin")
//    var path = zkClient.zk.exists("/deviceConfig/change", true)
//    if (path == null) {
//      zkClient.zk.create("/deviceConfig/change", null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
//    }
//    zkClient.zk.setData("/deviceConfig/change", "1".getBytes(), -1)
//    while (i < 100) {
//      val data = "datadata" + i
//
//      i += 1
//    }
//    log.info("end")
//    Thread.sleep(3000)


    val zkDec = new DeviceConfigMananger(new Properties())
    val map = zkDec.configMap

    new Thread(){
      override def run()= {
        try {
          val zkClient = new ZkClient("localhost", 2181, 2000, null)
          val path = zkClient.zk.exists("/deviceConfig/change", true)
          if (path == null) {
            zkClient.zk.create("/deviceConfig/change", "1".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
          }
//          var idx = 0
//          while (idx < 100) {
//            var stat: Stat = null
//            while ((stat = zkClient.zk.setData("/deviceConfig/change", ("" + idx).getBytes(), -1)) == null) {}
//            Thread.sleep(100)
//            idx += 1
//          }
//          println(stat.toString)
//          while ((stat = zkClient.zk.setData("/deviceConfig/change", "3".getBytes(), -1)) == null){}
//          println(stat.toString)
          Thread.sleep(100000)
        } catch {
          case e: Throwable =>
        }
      }
    }.start()
  }
}