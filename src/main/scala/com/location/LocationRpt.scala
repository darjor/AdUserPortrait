package com.location

import com.util.RptUtil
import org.apache.spark.sql.SparkSession

object LocationRpt {
  def main(args: Array[String]): Unit = {
    if (args.length != 2){
      println("输入目录不正确")
      sys.exit()
    }
    val Array(inputpath,outputpath) = args

    val spark = SparkSession
      .builder()
      .appName("ct")
      .master("local[*]")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    //获取数据
    val df = spark.read.parquet(inputpath)

    val res = df.rdd.map(row => {
      //根据指标字段获取数据
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")

      val winprice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")

      //处理请求数
      val rptList = RptUtil.ReqPt(requestmode,processnode)
      //处理展示点击
      val clickList = RptUtil.clickPt(requestmode,iseffective)
      //处理广告
      val adList = RptUtil.adPt(iseffective,isbilling,isbid,iswin,adorderid,winprice,adpayment)
      //所有指标
      val allList: List[Double] = rptList ++ clickList ++ adList
      ((row.getAs[String]("provincename"),row.getAs[String]("cityname")),allList)
    }).reduceByKey((list1,list2)=>{
      list1.zip(list2).map(t=>t._1+t._2)
    }).map(t=>t._1 + "," + t._2.mkString(","))
     .saveAsTextFile(outputpath)
  }
}
