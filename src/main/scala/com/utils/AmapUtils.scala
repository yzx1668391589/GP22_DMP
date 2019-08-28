package com.utils

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

/**
  * 商圈解析
  */

object AmapUtils {

  def getBussAmap(long:Double,lat:Double):String ={
    val location =long +","+lat

    val url =s"https://restapi.amap.com/v3/geocode/regeo?location="+location+"&key=a01290403bfe4ea818e0f38cc59e69c9&radius=1000&extensions=all"

    val jsonstr =HttpUtils.get(url)
    val jsonparse: JSONObject = JSON.parseObject(jsonstr)

    //判断状态是否成功
    val status: Int = jsonparse.getIntValue("status")

    if(status == 0 ) return ""

    //解析内部json串。
    val regeocodeJSON: JSONObject = jsonparse.getJSONObject("regeocode")
    if(regeocodeJSON ==null || regeocodeJSON.keySet().isEmpty) return ""

    //
    val address: JSONObject = regeocodeJSON.getJSONObject("addressComponent")
    if(address ==null || address.keySet().isEmpty) return ""

    val busniss: JSONArray = address.getJSONArray("businessAreas")
    if(busniss ==null || busniss.isEmpty) return null

    val buffer = collection.mutable.ListBuffer[String]()
    //循环输出
    for(item <-busniss.toArray){
      if(item.isInstanceOf[JSONObject]){
        val js: JSONObject = item.asInstanceOf[JSONObject]
        buffer.append(js.getString("name"))
      }
    }
    buffer.mkString(",")
  }
}
