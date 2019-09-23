package com.tags

import com.typesafe.config.ConfigFactory
import com.util.tagutil.TagsUtil
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession

/**
  * 上下文标签
  */

object ContextTags {
  def main(args: Array[String]): Unit = {
    if(args.length != 4){
      println("目录不正确")
      sys.exit()
    }

    val Array(inputpath,docs,stopwords,day) = args

    //创建上下文标签
    val spark = SparkSession.builder().appName("Tags")
      .master("local").getOrCreate()

    //调用HBaseAPI
    val load = ConfigFactory.load()
    //获取表名
    val hbaseTableName = load.getString("HBASE.tableName")
    //创建hadoop任务
    val configuration = spark.sparkContext.hadoopConfiguration
    //配置hbase连接
    configuration.set("hbase.zookeeper.quorum",load.getString("HBASE.Host"))
    //获取connection连接
    val hbconn = ConnectionFactory.createConnection(configuration)
    val hbadmin = hbconn.getAdmin
    //判断当前表是否可用
    if(!hbadmin.tableExists(TableName.valueOf(hbaseTableName))){
      println("当前表可用")
      //创建表对象
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))
      //创建列簇
      val columnDescriptor = new HColumnDescriptor("tags")
      //将创建的列簇加入表中
      tableDescriptor.addFamily(columnDescriptor)
      hbadmin.createTable(tableDescriptor)
      hbadmin.close()
      hbconn.close()
    }
    //指定输出类型和输出表
    val conf = new JobConf(configuration)
    conf.setOutputFormat(classOf[TableOutputFormat])
    conf.set(TableOutputFormat.OUTPUT_TABLE,hbaseTableName)

    //读取数据文件
    import spark.implicits._
    val df = spark.read.parquet(inputpath)

    //读取字典文件
    val docsRDD = spark.sparkContext.textFile(docs).map(_.split("\\s")).filter(_.length>=5)
        .map(arr=>(arr(4),arr(1))).collectAsMap()
    //广播字典
    val broadValue = spark.sparkContext.broadcast(docsRDD)
    //读取停用字典
    val stopwordsRDD = spark.sparkContext.textFile(stopwords).map((_,0)).collectAsMap()
    //广播停用字典
    val broadstopdict = spark.sparkContext.broadcast(stopwordsRDD)

    //获取所有ID
    val allUserId = df.rdd.map(row=>{
      val strList = TagsUtil.getAllUserId(row)
      (strList,row)
    })
    //构建点集合
    val verties = allUserId.flatMap(row=>{
      //获取所有数据
      val rows = row._2

      //标签实现
      //广告
      val adList = AdTags.makeTags(row)
      //媒体标签
      val appList = APPTags.makeTags(row)
      //设备标签
      val devList = DeviceTags.makeTags(row)
      //关键字标签
      val kList = KwordsTags.makeTags(row)
      //地域标签
      val locList = LocationTags.makeTags(row)
      //商圈
      val businessList = BusinessTag.makeTags(row)
      //获取所有的标签
      val tagList = adList ++ appList ++ devList ++ kList ++ locList ++ businessList

      //保留用户ID
      val VD = row._1.map((_,0)) ++ tagList
      //处理用户ID的字符串并保证只有其中一个ID携带用户标签
      row._1.map(uId=>{
        if(row._1.head.equals(uId)) {
          (uId.hashCode.toLong, VD)
        }else{
          (uId.hashCode.toLong,List.empty)
        }
      })
    })

    // 打印
    //verties.take(20).foreach(println)
    // 构建边的集合
    val edges = allUserId.flatMap(row=>{
      // A B C: A->B  A ->C
      row._1.map(uId=>Edge(row._1.head.hashCode.toLong,uId.hashCode.toLong,0))
    })
    //edges.foreach(println)
    // 构建图
    val graph = Graph(verties,edges)
    // 根据图计算中的连通图算法，通过图中的分支，连通所有的点
    // 然后在根据所有点，找到内部最小的点，为当前的公共点
    val vertices = graph.connectedComponents().vertices
    // 聚合所有的标签
    vertices.join(verties).map{
      case (uid,(cnId,tagsAndUserId))=>{
        (cnId,tagsAndUserId)
      }
    }.reduceByKey(
      (list1,list2)=>{
        (list1++list2)
          .groupBy(_._1)
          .mapValues(_.map(_._2).sum)
          .toList
      }).map{
      case (userId,userTags) =>{
        // 设置rowkey和列、列名
        val put = new Put(Bytes.toBytes(userId))
        put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes(day),Bytes.toBytes(userTags.mkString(",")))
        (new ImmutableBytesWritable(),put)
      }
    }.saveAsHadoopDataset(conf)

    spark.stop()
  }
}
