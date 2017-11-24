package util

import javaclz.JsonField

import net.sf.json.JSONObject

/**
  * Created by xiaoke on 17-6-6.
  */

//joTmp.put("dcode", id)
//joTmp.put("uid", dei.getUid)
//joTmp.put("aid", dei.getAid)
//joTmp.put("sid", dei.getSid)
//joTmp.put("pid", dei.getPid)
//joTmp.put("unit", dei.getPoutunit)
//joTmp.put("st", ptimeStamp)


object AvgPersistenceUtil {
  def avgPersistenceObj(dcode: String,
                        uid: Int,
                        aid: Int,
                        sid: Int,
                        pid: Int,
                        unit: String,
                        st: Long,
                        avgName: String,
                        sum: Double,
                        num: Int
                       ) = {
    val avgObj = new JSONObject()
    avgObj.put("dcode", dcode)
    avgObj.put("uid", uid)
    avgObj.put("aid", aid)
    avgObj.put("sid", sid)
    avgObj.put("pid", pid)
    avgObj.put("unit", unit)
    avgObj.put("st", st)
    avgObj.put("avgName", avgName)
    avgObj.put("sum", sum)
    avgObj.put("num", num)
    avgObj
  }
}
