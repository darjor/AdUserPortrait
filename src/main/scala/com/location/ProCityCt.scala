package com.location

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}

object ProCityCt {
  def main(args: Array[String]): Unit = {
    if (args.length != 1){
      println("输入目录不正确")
      sys.exit()
    }

    val Array(inputpath) = args

    val spark = SparkSession
      .builder()
      .appName("ct")
      .master("local")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    //获取数据
    val df = spark.read.parquet(inputpath)
    //注册临时视图
    df.createTempView("log")
    val df2 = spark
      .sql("select provincename,cityname,count(*) ct from log group by provincename,cityname")
    //df2.show()
    //存储为json格式
    //df2.write.partitionBy("provincename","cityname").json("1")
      //通过config配置文件依赖进行加载相关的配置信息
    val load = ConfigFactory.load()
    //创建properties对象
    val pro = new Properties()
    pro.setProperty("user",load.getString("jdbc.user"))
    pro.setProperty("password",load.getString("jdbc.password"))
    //存储
    df2.write.mode(SaveMode.Append).jdbc(
      load.getString("jdbc.url"),load.getString("jdbc.tableName"),pro)
    spark.stop()
  }
}
