package metric

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.Logging

/**
  * Created by xiaoke on 17-8-9.
  */
trait MetricInfo[T] extends Serializable{

  def infoIn(merticInfo: T): Unit

  def unknownError = throw new UnknownError("Unknown Metric Info")
}

class NumMetric extends MetricInfo[Long] {

  val numStatistics = new AtomicLong(0)

  override def infoIn(merticInfo: Long): Unit = numStatistics.addAndGet(merticInfo)
}

class MsgMetric extends MetricInfo[(Long, Long)]{

  val totalNum = new AtomicLong(0)

  val todayNum = new AtomicLong(0)

  val totalBytes = new AtomicLong(0)

  val todayBytes = new AtomicLong(0)

  @volatile private var daySt = System.currentTimeMillis() / 86400000

  override def infoIn(metricInfo: (Long, Long)) = synchronized {
    val todaySt = System.currentTimeMillis() / 86400000
    if (todaySt != daySt) {
      todayNum.set(0)
      todayBytes.set(0)
      daySt = todaySt
    }
    totalNum.addAndGet(metricInfo._1)
    todayNum.addAndGet(metricInfo._1)
    totalBytes.addAndGet(metricInfo._2)
    todayBytes.addAndGet(metricInfo._2)
  }
}