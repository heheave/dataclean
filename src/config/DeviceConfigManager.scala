package config

import java.io.{FileFilter, File}
import java.util
import java.util.concurrent.atomic.AtomicLong
import javaclz.{JavaV, JavaConfigure}

import action._
import org.apache.log4j.Logger

import scala.io.Source

/**
  * Created by xiaoke on 17-6-1.
  */

class DeviceConfigManager(javaConfigure: JavaConfigure) {

  private val log = Logger.getLogger(this.getClass)

  private val lastScan = new AtomicLong(0)

  private val configBasePath = javaConfigure.getStringOrElse(JavaV.DEVICE_CONFIG_PATH, "dconfig")

  private val config: Config = new Config()

  def configFromPath(path: String = configBasePath) = {
    val truePath = if (path == null) configBasePath else path
    val file = new File(truePath)
    if (file.exists() && file.isDirectory) {
      val lastScanTime = lastScan.get()
      var maxScanTime: Long = lastScanTime
      val deviceIds = new util.HashSet[String]()
      val filteredFiles = file.listFiles(new FileFilter {
        override def accept(filterFile: File): Boolean = {
          val fileFilterLastModified = filterFile.lastModified
          if (fileFilterLastModified > lastScanTime) {
            if (maxScanTime < fileFilterLastModified) {
              maxScanTime = fileFilterLastModified
            }
            filterFile.isFile
          } else {
            if (filterFile.isFile) {
              deviceIds.add(filterFile.getName)
            }
            false
          }
        }
      })
      lastScan.set(maxScanTime)
      val newConfig = new Config
      filteredFiles.foreach(f => {
        parseOneFile(f, newConfig)
      })
      config.updateByConfig(deviceIds, newConfig)
    }
    config
  }

  private def avgType(avg: String) = avg match {
    case "DAY" => Actions.AVG_DAY
    case "HOUR" => Actions.AVG_HOUR
    case "MIN" => Actions.AVG_MIN
    case _ => Actions.AVG_NONE
  }

  private def parseOneFile(file: File, config: Config): Unit = {
    val did = file.getName
    for (line <- Source.fromFile(file).getLines()) try {
      val lines = line.split(":")
      val at = avgType(lines(lines.length - 1))
      val port = lines(0).toInt
      val action = lines(1) match {
        case "NEG" => NegAction(avg = at)
        case "ADD" => AddAction(lines(2).toDouble, avg = at)
        case "SUB" => SubAction(lines(2).toDouble, avg = at)
        case "MUL" => MulAction(lines(2).toDouble, avg = at)
        case "DIV" => DivAction(lines(2).toDouble, avg = at)
        case "RVS" => RvsAction(avg = at)
        case _ => {
          log.warn("Unsupported action: %s".format(lines(1)))
          null
        }
      }
      if (action != null) {
        config.append(did, port, action.asInstanceOf[Action])
      }
    } catch {
      case e: Throwable => log.info("Could not apply action by line: %s".format(line), e)
    }
  }

}

object DeviceConfigManager {

  @volatile private var dcf: DeviceConfigManager = null

  def apply(javaConfigure: JavaConfigure): DeviceConfigManager = {
    if (dcf == null) {
      DeviceConfigManager.getClass.synchronized{
        if (dcf == null) {
          dcf = new DeviceConfigManager(javaConfigure)
        }
      }
    }
    dcf
  }

}
