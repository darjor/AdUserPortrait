package com.tags

import com.util.tagutil.Tag
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

/**
  * 地域标签
  */
object LocationTags extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()

    val row = args(0).asInstanceOf[Row]
    val province = row.getAs[String]("provincename")
    val city = row.getAs[String]("cityname")
    if(StringUtils.isNotEmpty(province)){
      list:+=("ZP"+province,1)
    }
    if(StringUtils.isNotEmpty(city)){
      list:+=("ZC"+city,1)
    }
    list
  }
}
