package metric

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.Logging

/**
  * Created by xiaoke on 17-8-9.
  */
trait MetricInfo[T] {

  def infoIn(merticInfo: T): Unit

  def unknownError = throw new UnknownError("Unknown Metric Info")
}

class NumMetric extends MetricInfo[Long] {

  val numStatistics = new AtomicLong(0)

  override def infoIn(merticInfo: Long): Unit = numStatistics.addAndGet(merticInfo)
}

class AppMetric(val app: String) extends MetricInfo[AnyRef] with Serializable{

  val messageNum = new AtomicLong(0)

  val invalidNum = new AtomicLong(0)

  val validNum = new AtomicLong(0)

  val configNum = new AtomicLong(0)

  val unconfigNum = new AtomicLong(0)

  override def infoIn(merticInfo: AnyRef): Unit = {
    merticInfo match {
      case null =>
      case mi: AppMetric => {
        messageNum.addAndGet(mi.messageNum.get())
        validNum.addAndGet(mi.validNum.get())
        invalidNum.addAndGet(mi.invalidNum.get())
        configNum.addAndGet(mi.configNum.get())
        unconfigNum.addAndGet(mi.unconfigNum.get())
      }

      case aai: AppAccumulatorInfo => {
        messageNum.addAndGet(aai.mn)
        validNum.addAndGet(aai.vp)
        invalidNum.addAndGet(aai.ip)
        configNum.addAndGet(aai.cp)
        unconfigNum.addAndGet(aai.up)
      }
      case _ =>
    }
  }
}