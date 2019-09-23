
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object test1 {
  /**
    * json数据归纳格式（考虑获取到数据的成功因素 status=1成功 starts=0 失败）：
    * 1、按照pois，分类businessarea，并统计每个businessarea的总数。
    * 2、按照pois，分类type，为每一个Type类型打上标签，统计各标签的数量
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val path =sc.textFile("F:\\资料\\千锋资料\\BigData-23\\spark\\试题\\json.txt")
    val result: RDD[String] = path.map(t => {
      // 创建集合 保存数据
      val list = collection.mutable.ListBuffer[String]()

      val jsonparse = JSON.parseObject(t)
      // 判断状态是否为1
      val status = jsonparse.getIntValue("status")
      if (status == 1) {
        // 解析内部json串
        val regeocodeJson = jsonparse.getJSONObject("regeocode")
        if (regeocodeJson != null && !regeocodeJson.keySet().isEmpty) {
          //获取pois数组
          val poisArray = regeocodeJson.getJSONArray("pois")
          if (poisArray != null && !poisArray.isEmpty) {
            // 循环输出获取businessarea
            for (item <- poisArray.toArray) {
              if (item.isInstanceOf[JSONObject]) {
                val json = item.asInstanceOf[JSONObject]
                list.append(json.getString("businessarea"))
              }
            }
          }
        }
      }
      list.mkString(",")
    })
    result.flatMap(line=>{
      line.split(",").map((_,1))
    }).reduceByKey(_+_).foreach(println)
  }
}
