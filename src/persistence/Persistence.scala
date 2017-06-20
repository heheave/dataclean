package persistence

import java.util
import java.util.Properties
import javaclz.persist.config.HConf
import javaclz.persist.{PersistenceLevel, AdapterPersistence}
import javaclz.persist.data.{PersistenceDataJsonWrap, PersistenceData}
import javaclz.persist.opt.PersistenceOpt

import net.sf.json.JSONObject

/**
  * Created by xiaoke on 17-6-4.
  */
class PersistenceSink(fun: () => AdapterPersistence) extends Serializable {

  private lazy implicit val joToPd = (jo: JSONObject) => new PersistenceDataJsonWrap(jo)

  private lazy val persistence = fun()

  def once(pd: PersistenceData, persistenceOpt: PersistenceOpt, pLevel: PersistenceLevel = PersistenceLevel.BOTH): Unit = {
    persistence.persistence(pd, persistenceOpt, pLevel)
  }

  def batch(pds: util.Collection[PersistenceData], persistenceOpt: PersistenceOpt, pLevel: PersistenceLevel = PersistenceLevel.BOTH): Unit = {
    try {
      persistence.persistence(pds, persistenceOpt, pLevel)
    } finally {
      pds.clear()
    }
  }
}


object PersistenceSink {
  def apply(hconf: HConf, conf: Properties): PersistenceSink = {
    val f = () => {
      val pSink = new AdapterPersistence(hconf.hconf(), conf)
      pSink.start()
      sys.addShutdownHook{
        pSink.stop()
      }
      pSink
    }
    new PersistenceSink(f)
  }
}
