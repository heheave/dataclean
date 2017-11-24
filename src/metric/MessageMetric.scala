package metric

import java.util
import java.util.concurrent.atomic.{AtomicLong}

import org.apache.spark.Logging

/**
  * Created by xiaoke on 17-7-17.
  */

// this object is used to store some metric information
object MessageMetric extends Logging {

  val totalNum = new NumMetric

  val todayNum = new NumMetric

  private val tenantMetric = new util.HashMap[Int, MsgMetric]()

  private val appMetric = new util.HashMap[Int, MsgMetric]()

  private val sceneMetric = new util.HashMap[Int, MsgMetric]()

  private val pointerMetric = new util.HashMap[Int, MsgMetric]()

  def getTenatMetricById(id: String): Option[MsgMetric] = tenantMetric.synchronized {
    val metric = tenantMetric.get(id)
    metric match {
      case null => None
      case _ => Some(metric)
    }
  }

  def getAppMetricById(id: String): Option[MsgMetric] = appMetric.synchronized {
    val metric = appMetric.get(id)
    metric match {
      case null => None
      case _ => Some(metric)
    }
  }

  def getSceneMetricById(id: String): Option[MsgMetric] = sceneMetric.synchronized {
    val metric = sceneMetric.get(id)
    metric match {
      case null => None
      case _ => Some(metric)
    }
  }

  def getPointerMetricById(id: String): Option[MsgMetric] = pointerMetric.synchronized {
    val metric = pointerMetric.get(id)
    metric match {
      case null => None
      case _ => Some(metric)
    }
  }

  def tenantInfoIn(tenantValues: util.List[AppAccumulatorInfo]): Unit = tenantMetric.synchronized {
    if (tenantValues != null) {
      tenantValues.synchronized {
        val iter = tenantValues.iterator()
        while (iter.hasNext) {
          val v = iter.next()
          var metric = tenantMetric.get(v.id)
          if (metric == null) {
            metric = new MsgMetric()
            tenantMetric.put(v.id, metric)
          }
          metric.infoIn((v.mn, v.mb))
        }
        tenantValues.clear()
      }
    }
  }

  def appInfoIn(appValues: util.List[AppAccumulatorInfo]): Unit = appMetric.synchronized {
    if (appValues != null) {
      appValues.synchronized {
        val iter = appValues.iterator()
        while (iter.hasNext) {
          val v = iter.next()
          var metric = appMetric.get(v.id)
          if (metric == null) {
            metric = new MsgMetric()
            appMetric.put(v.id, metric)
          }
          metric.infoIn((v.mn, v.mb))
        }
        appValues.clear()
      }
    }
  }

  def sceneInfoIn(sceneValues: util.List[AppAccumulatorInfo]): Unit = sceneMetric.synchronized {
    if (sceneValues != null) {
      sceneValues.synchronized {
        val iter = sceneValues.iterator()
        while (iter.hasNext) {
          val v = iter.next()
          var metric = sceneMetric.get(v.id)
          if (metric == null) {
            metric = new MsgMetric()
            sceneMetric.put(v.id, metric)
          }
          metric.infoIn((v.mn, v.mb))
        }
        sceneValues.clear()
      }
    }
  }

  def pointerInfoIn(pointerValues: util.List[AppAccumulatorInfo]): Unit = pointerMetric.synchronized {
    if (pointerValues != null) {
      pointerValues.synchronized {
        val iter = pointerValues.iterator()
        while (iter.hasNext) {
          val v = iter.next()
          var metric = pointerMetric.get(v.id)
          if (metric == null) {
            metric = new MsgMetric()
            pointerMetric.put(v.id, metric)
          }
          metric.infoIn((v.mn, v.mb))
        }
        pointerValues.clear()
      }
    }
  }

  def showInfos(idx: Int): Unit = {
    val (title, map) = idx match {
      case 0 => ("tenant", tenantMetric)
      case 1 => ("app", appMetric)
      case 2 => ("scene", sceneMetric)
      case 3 => ("pointer", pointerMetric)
      case _ => null
    }
    if (map == null) {
      logInfo("idx should be 0 ~ 4")
    } else {
      map.synchronized{
        val iter = map.entrySet().iterator()
        while (iter.hasNext) {
          val entry = iter.next()
          logInfo("%s-%s: %s,%s,%s,%s".format(
            title,
            entry.getKey,
            entry.getValue.totalNum.get(),
            entry.getValue.totalBytes.get(),
            entry.getValue.todayNum.get(),
            entry.getValue.todayBytes.get()
          ))
        }
      }
    }
  }

}
