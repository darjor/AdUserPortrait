package com.location

import com.util.RptUtil
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession

class APP {

}
object APP{
  def main(args: Array[String]): Unit = {
    if (args.length != 2){
      println("输入目录不正确")
      sys.exit()
    }
    val Array(inputpath,outputhpath,docs) = args

    val spark = SparkSession
      .builder()
      .appName("ct")
      .master("local[*]")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    //读取数据字典
    val docMap = spark.sparkContext.textFile(docs).map(_.split("\\s", -1)).
      filter(_.length >= 5).map(arr => (arr(4), arr(1))).collectAsMap()

    //广播
    val broadcast = spark.sparkContext.broadcast(docMap)
    //读取数据文件
    val df = spark.read.parquet(inputpath)
    import spark.implicits._
    df.map(row => {

      //取媒体相关字段
      val appName = row.getAs[String]("appName")
      if(StringUtils.isBlank(appName)){

      }
      //根据指标字段获取数据
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorrderid")

      val winprice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")

      //处理请求数
      val rptList = RptUtil.ReqPt(requestmode,processnode)
      //处理展示点击
      val clickList = RptUtil.clickPt(requestmode,iseffective)
      //处理广告
      val adList = RptUtil.adPt(iseffective,isbilling,isbid,iswin,adorderid,winprice,adpayment)
      //所有指标
      //所有指标
      val allList: List[Double] = rptList ++ clickList ++ adList
      ((row.getAs[String]("provincename"),row.getAs[String]("cityname")),allList)
    }).rdd.reduceByKey((list1,list2)=>{
      list1.zip(list2).map(t=>t._1+t._2)
    }).map(t=>t._1 + "," + t._2.mkString(","))
      .saveAsTextFile(outputhpath)
  }
}
