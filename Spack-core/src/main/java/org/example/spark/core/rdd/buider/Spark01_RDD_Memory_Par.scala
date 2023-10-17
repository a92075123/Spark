package org.example.spark.core.rdd.buider

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory_Par {
  def main(args: Array[String]): Unit = {
    //TODO 準備環境
    //local[*]=你有幾核心就用幾核
    //setAppName幫應用程序取名字
      val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
      sparkConf.set("spark.default.parallelism","5")
      val sc= new SparkContext(sparkConf)

    //TODO 創建RDD
    //RDD的並行度&分區
    //makeRDD方法可以傳遞第2個參數，這個參數表示分區的數量
    //第2個參數可以不傳遞，那麼RDD會使用默認值
    //val rdd = sc.makeRDD(List(1,2,3,4),2)
    //spark在默認情況下，從配置對象中獲取配置參數:spark.default.parallelism
    //如果獲取不到，那就使用totalCores屬性，這個屬性取值為當前運行的最大核數
    val rdd = sc.makeRDD(List(1,2,3,4))
    //將處理的數據保存成分區文件
    rdd.saveAsTextFile("output")


    //TODO 關閉環境
      sc.stop()
  }
}
