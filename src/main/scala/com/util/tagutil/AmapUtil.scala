package com.util.tagutil

import com.alibaba.fastjson.{JSON, JSONObject}

/**
  * 从高德地图获取商圈信息
  */
object AmapUtil {

  /**
    * 解析经纬度
    * @param long
    * @param lat
    * @return
    */

  def getBusinessFromAmap(long:Double,lat:Double):String={
    val location = long + "," + lat
    //获取url
    val url = "https://restapi.amap.com/v3/geocode/regeo?location=" + location + "&key=a45e0c03a4dc6180877224e17b426dd8"
    //调用接口发送请求
    val jsonstr = HTTPUtil.get(url)
    //j解析json串
    val jSONObject1 = JSON.parseObject(jsonstr)
    //判断当前状态是否为1
    val status = jSONObject1.getIntValue("status")
    if(status == 0) return ""
    //如果不为空
    val jSONObject = jSONObject1.getJSONObject("regeocode")
    if(jSONObject == null) return  ""
    val jSONObject2 = jSONObject.getJSONObject("addressComponent")
    if(jSONObject2 == null) return  ""
    val jSONArray = jSONObject2.getJSONArray("businessAreas")
    if(jSONArray == null) return  ""

    //定义集合取值
    val result = collection.mutable.ListBuffer[String]()
    //循环数组
    for(item <- jSONArray.toArray()){
      if(item.isInstanceOf[JSONObject]){
        val json = item.asInstanceOf[JSONObject]
        val name = json.getString("name")
        result.append(name)
      }
    }
    result.mkString(",")

  }
}
