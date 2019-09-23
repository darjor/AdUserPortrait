package com.tags

import com.util.tagutil.Tag
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

/**
  * 渠道标签
  */
object ChannelTags extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    //获取数据类型
    val row = args(0).asInstanceOf[Row]
    //获取渠道名称和类型
    val channel = row.getAs[String]("adplatformproviderid")
    if(StringUtils.isNotEmpty(channel)){
      list:+=("CN" + channel,1)
    }
    list
  }
}
