package com.tags

import com.util.tagutil.TagsUtil
import org.apache.spark.sql.SparkSession

/**
  * 上下文标签
  */

object TagsContext {
  def main(args: Array[String]): Unit = {
    if(args.length != 1){
      println("目录不正确")
      sys.exit()
    }

    val Array(inputpath) = args

    //创建上下文标签
    val spark = SparkSession.builder().appName("Tags")
      .master("local").getOrCreate()
    //读取数据文件
    import spark.implicits._
    val df = spark.read.parquet(inputpath)

    //处理信息
    df.map(row=>{
      //获取用户唯一的id
      val userId = TagsUtil.getOneUserId(row)
      //标签实现
      //广告
      val adList = TagsAd.makeTags(row)
      //商圈
      val businessList = BusinessTag.makeTags(row)
      (userId,businessList)
    }).rdd.foreach(println)
  }
}
