package org.example.spark.core.rdd.buider

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory_Par1 {
  def main(args: Array[String]): Unit = {
    //TODO 準備環境
    //local[*]=你有幾核心就用幾核
    //setAppName幫應用程序取名字
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 創建RDD

    //[1,2],[3,4]
    //val rdd = sc.makeRDD(List(1, 2, 3, 4),2)
    //[1],[2],[3,4]
    //val rdd = sc.makeRDD(List(1, 2, 3, 4),3)
    //[1],[2,3],[4,5]
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5), 3)


    //將處理的數據保存成分區文件
    rdd.saveAsTextFile("output")


    //TODO 關閉環境
    sc.stop()
  }
}
