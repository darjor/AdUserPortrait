package com.tags

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession

object AppName {
  def main(args: Array[String]): Unit = {
    val Array(inputpath,docs) = args
    val spark = SparkSession
      .builder()
      .appName("ct")
      .master("local[*]")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val docMap = spark.sparkContext.textFile(docs).map(_.split("\\s", -1)).
      filter(_.length >= 5).map(arr => (arr(4), arr(1))).collectAsMap()
    val broad =spark.sparkContext.broadcast(docMap)

    val df = spark.read.parquet(inputpath)
    val res = df.rdd.map((row)=>{
      var appName = row.getAs[String]("appname")
      if(StringUtils.isBlank(appName)){
        appName = broad.value.getOrElse(row.getAs[String]("appid"),"null")
      }
      ("APP"+appName,1)
    })
    print(res.collect().toMap)
  }
}
