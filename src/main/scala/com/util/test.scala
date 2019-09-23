package com.util

import com.util.tagutil.HTTPUtil
import org.apache.spark.sql.SparkSession

object test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("httptest").master("local[*]").getOrCreate()

    val arr = Array("https://restapi.amap.com/v3/geocode/regeo?&location=116.310003,39.991957&key=a45e0c03a4dc6180877224e17b426dd8&extensions=all")

    val rdd = spark.sparkContext.makeRDD(arr)
    rdd.map(t=>{
      HTTPUtil.get(t)
    }).foreach(println)

  }
}
