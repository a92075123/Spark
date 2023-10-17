package org.example.spark.core.rdd.buider

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory {
  def main(args: Array[String]): Unit = {
    //TODO 準備環境
    //local[*]=你有幾核心就用幾核
    //setAppName幫應用程序取名字
      val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
      val sc= new SparkContext(sparkConf)
    //TODO 創建RDD
    /*
    從內存中創建RDD，將內存中集合的數據作為處理的數據源
    要幫parallelize準備數據，所以創造出Seq來當作數據源
    只有觸發collect才會執行應用程序
    parallelize:並行
    makeRDD:是parallelize的簡寫，方法裡會調用parallelize
     */
    val seq =Seq[Int](1,2,3,4)
    //    val rdd: RDD[Int] = sc.parallelize(seq)

    val rdd: RDD[Int] = sc.makeRDD(seq)
        rdd.collect().foreach(println)

    //TODO 關閉環境
      sc.stop()
  }
}
