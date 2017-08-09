package metric

import java.util
import java.util.concurrent.atomic.{AtomicLong}

/**
  * Created by xiaoke on 17-7-17.
  */

// this object is used to store some metric information
object MessageMetric {

  val validNum = new NumMetric

  val invalidNum = new NumMetric

  private val appStatisticsMap = new util.HashMap[String, AppMetric]()

  def getMetricByAppName(app: String): Option[AppMetric] = appStatisticsMap.synchronized {
    val metric = appStatisticsMap.get(app)
    metric match {
      case null => None
      case _ => Some(metric)
    }
  }

  def appMetricInfoIn(appValues: util.List[AppAccumulatorInfo]): Unit = appStatisticsMap.synchronized {
    if (appValues != null) {
      appValues.synchronized {
        val iter = appValues.iterator()
        while (iter.hasNext) {
          val v = iter.next()
          val metric = appStatisticsMap.get(v.app)
          if (metric == null) {
            val newAppMetric = new AppMetric(v.app)
            newAppMetric.infoIn(v)
            appStatisticsMap.put(v.app, newAppMetric)
          } else {
            metric.infoIn(v)
          }
        }
        appValues.clear()
      }
    }
  }

  def appMetricRemove(app: String): Option[AppMetric] = appStatisticsMap.synchronized {
    val metric = appStatisticsMap.remove(app)
    metric match {
      case null => None
      case _ => Some(metric)
    }
  }

  def appMetricClear(): Unit = appStatisticsMap.synchronized {
    appStatisticsMap.clear()
  }

}
