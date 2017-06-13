package util

import javaclz.JsonField

import net.sf.json.JSONObject

/**
  * Created by xiaoke on 17-6-6.
  */
object AvgPersistenceUtil {
  def avgPersistenceObj(id: String,
                        portIdx: Int,
                        avgName: String,
                        dTimeStamp: Long,
                        ptimeStamp: Long,
                        sum: Double,
                        num: Int
                       ) = {
    val avgObj = new JSONObject()
    avgObj.put(JsonField.DeviceValue.ID, id);
    avgObj.put(JsonField.DeviceValue.PORTID, portIdx);
    avgObj.put(JsonField.DeviceValue.AVGTYPE, avgName)
    avgObj.put(JsonField.DeviceValue.DTIMESTAMP, dTimeStamp)
    avgObj.put(JsonField.DeviceValue.PTIMESTAMP, ptimeStamp)
    avgObj.put(JsonField.DeviceValue.SUMV, sum)
    avgObj.put(JsonField.DeviceValue.NUMV, num)
    avgObj
  }
}
