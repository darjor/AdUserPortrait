package com.tags

import ch.hsr.geohash.GeoHash
import com.util.String2Type
import com.util.tagutil.{AmapUtil, JedisConnectionPool, Tag}
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

/**
  * 商圈标签
  */
object BusinessTag extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    //获取数据
    val row = args(0).asInstanceOf[Row]
    //获取经纬度
    if(String2Type.toDouble(row.getAs[String]("long")) >= 73
    && String2Type.toDouble(row.getAs[String]("long")) <= 136
    && String2Type.toDouble(row.getAs[String]("lat")) >= 3
    && String2Type.toDouble(row.getAs[String]("lat"))<53){
      val long = row.getAs[String]("long").toDouble
      val lat = row.getAs[String]("lat").toDouble
      //获取商圈名称
      val business = getBusiness(long,lat)
      if(StringUtils.isNotBlank(business)){
        val str = business.split(",")
        str.foreach(str=>{
          list:+=(str,1)
        })
      }
    }
    list
  }

  /**
    * 获取商圈信息
    */
  def getBusiness(long:Double,lat:Double):String={
    //GeoHash码
    val geohash = GeoHash.geoHashStringWithCharacterPrecision(lat,long,6)
    //数据库查询当前商圈信息
    var business = redis_queryBisiness(geohash)
    if(business == null){
      //去高德请求
      business = AmapUtil.getBusinessFromAmap(long,lat)
      //将商圈信息插入到数据库
      if (business != null && business.length > 0){
        redis_insterBusiness(geohash,business)
      }
    }
    business
  }

  /**
    * 数据库获取商圈信息
    * @param geohash
    * @return
    */
  def redis_queryBisiness(geohash:String):String={
    val jedis = JedisConnectionPool.getConnection()
    val business = jedis.get(geohash)
    jedis.close()
    business
  }

  /**
    * 将商圈信息保存到数据库
    * @param geohash
    * @param business
    * @return
    */
  def redis_insterBusiness(geohash:String,business:String):Unit={
    val jedis = JedisConnectionPool.getConnection()
    jedis.set(geohash,business)
    jedis.close()
  }
}
