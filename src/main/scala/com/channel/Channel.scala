package com.channel

import com.util.terminalutil.TerminalUtil
import org.apache.spark.sql.SparkSession

object Channel {
  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      println("目录不正确")
      sys.exit()
    }

    val Array(inputpath,outputpath) = args

    val spark = SparkSession.builder()
      .appName("facility")
      .master("local")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    //获取数据
    val df = spark.read.parquet(inputpath)

    df.rdd.map(row=>{
      //获取指标字段
      //REQUESTMODE	PROCESSNODE	ISEFFECTIVE	ISBILLING	ISBID	ISWIN	ADORDEERID

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
      val reqList = TerminalUtil.ReqPt(requestmode,processnode)
      //处理点击
      val cliList = TerminalUtil.clickPt(requestmode,iseffective)
      //处理广告
      val adList = TerminalUtil.adPt(iseffective,isbilling,isbid,iswin,adorderid,winprice,adpayment)
      //所有指标
      val allList = reqList ++ cliList ++ adList

      ((row.getAs[String]("channelid")),allList)

    }).reduceByKey((list1,list2)=>{
      list1.zip(list2).map(t=>t._1 + t._2)
    }).map(t=>t._1+","+t._2.mkString(","))
      .saveAsTextFile(outputpath)
  }
}
