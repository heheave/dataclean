package avgcache

import java.util.concurrent.{TimeUnit, Executors, ConcurrentHashMap}

import org.apache.log4j.Logger


/**
  * Created by xiaoke on 17-6-2.
  */

class CacheInfo(btime: String) {

  private var sumD: Double = 0

  private var sumN: Int = 0

  val beginTime = btime

  def add(d: Double) = synchronized {
    sumD += d
    sumN += 1
  }

  def sumData = sumD

  def sumNum = sumN

  override def hashCode(): Int = if (beginTime != null) beginTime.hashCode else 0

  override def equals(obj: scala.Any): Boolean = obj match {
    case o: CacheInfo => beginTime.equals(o.beginTime)
    case _ => false
  }
}

case class CacheKey(d: String, p: Int) {

  val did = d

  val pidx = p

  override def equals(other: Any): Boolean = other match {
    case that: CacheKey =>
        did == that.did &&
        pidx == that.pidx
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(did, pidx)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

sealed class AvgCache(avg: Avg) {

  private val log = Logger.getLogger(this.getClass)

  lazy val excutorPool = Executors.newSingleThreadScheduledExecutor()

  lazy val avgMap = new ConcurrentHashMap[CacheKey, CacheInfo]()

  startCheckRunner()

  private def startCheckRunner(): Unit = {
    val interval = avg.getInterval()
    log.info("-----------------------------: Begin to startCheckRunner")
    excutorPool.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        val bt = System.currentTimeMillis()
        log.info("-----------------------------: Begin startCheckRunner" + bt)
        try {
          val entryIter = avgMap.entrySet().iterator()
          val curTime = System.currentTimeMillis() - (interval << 1)
          val curTimeStr = avg.format(curTime)
          if (entryIter.hasNext) {
            val toRemovedKey = new java.util.HashMap[CacheKey, CacheInfo]()
            while (entryIter.hasNext) {
              val entry = entryIter.next()
              if (entry.getValue != null && curTimeStr.compareTo(entry.getValue.beginTime) > 0) {
                 toRemovedKey.put(entry.getKey, entry.getValue)
              }
            }

            if (!toRemovedKey.isEmpty) {
              val toRmIter = toRemovedKey.entrySet().iterator()
              while (toRmIter.hasNext) {
                val toRmEntry = toRmIter.next()
                avgMap.remove(toRmEntry.getKey, toRmEntry.getValue)
              }
              toRemovedKey.clear()
            }
          }

        } catch {
          case e: Throwable => log.warn("Schedule at " + avg.avgType(), e)
        }
        val et = System.currentTimeMillis()
        log.info("-----------------------------: End startCheckRunner" + (et - bt))
      }
    }, interval, interval << 1, TimeUnit.MILLISECONDS)

    sys.addShutdownHook {
      log.info("-----------------------------: Stop startCheckRunner")
      excutorPool.shutdown()
      avgMap.clear()
    }
  }

  def avgName = avg.avgType()

  def addData(did: String, pidx: Int, timestamp: Long, data: Double): CacheInfo = {
    val key = CacheKey(did, pidx)
    val cacheInfo = avgMap.get(key)
    val ctime = avg.format(timestamp)
    log.info("-----------------------------ctime: " + ctime)
    if (cacheInfo != null) {
      val btime = cacheInfo.beginTime
      log.info("-----------------------------btime: " + btime)
      if (ctime.equals(btime)) {
        cacheInfo.add(data)
        null
      } else {
        val newCacheInfo = new CacheInfo(ctime)
        newCacheInfo.add(data)
        avgMap.put(key, newCacheInfo)
        cacheInfo
      }
    } else {
      val newCacheInfo = new CacheInfo(ctime)
      newCacheInfo.add(data)
      avgMap.put(key, newCacheInfo)
      null
    }
  }
}

class AvgCacheTool(fun: () => AvgCache) extends Serializable{
  lazy val avgCache = fun()

  def avgName = avgCache.avgName

  def addData(did: String, pidx: Int, timestamp: Long, data: Double): CacheInfo = avgCache.addData(did, pidx, timestamp, data)
}


object AvgCacheManager {
  def applyHour(): AvgCacheTool = {
    val f = () => {
      new AvgCache(HourAvg())
    }
    new AvgCacheTool(f)
  }

  def applyMin(): AvgCacheTool = {
    val f = () => {
      new AvgCache(MinAvg())
    }
    new AvgCacheTool(f)
  }

  def applyDay(): AvgCacheTool = {
    val f = () => {
      new AvgCache(DayAvg())
    }
    new AvgCacheTool(f)
  }
}
