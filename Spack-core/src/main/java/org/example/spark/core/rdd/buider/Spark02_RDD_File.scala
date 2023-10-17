package org.example.spark.core.rdd.buider

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File {
  def main(args: Array[String]): Unit = {
    //TODO 準備環境
    //local[*]=你有幾核心就用幾核
    //setAppName幫應用程序取名字
      val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
      val sc= new SparkContext(sparkConf)
    //TODO 創建RDD
    /*
    從文件中創建RDD，將文件中的數據做為處理的數據源
    textFile的路徑為當前環境的根路徑為主，可以學絕對路徑，也可以寫相對路徑
     sc.textFile("D:\\workspace2\\Spark\\Spack-core\\datas\\1.txt");
     */
    //相對路徑
    //    val rdd: RDD[String] = sc.textFile("datas/1.txt")

    //路徑可以是文件的具體路徑，或是目錄名稱
    //var rdd = sc.textFile("datas")

    //路徑還可以使用通配符*
    val rdd = sc.textFile("datas/1*.txt")
    //路徑還可是分布式儲存系統路徑:HDFS
    //val rdd= sc.textFile("hdfs://linux:8020/test.txt")

    rdd.collect().foreach(println)



    //TODO 關閉環境
      sc.stop()
  }
}
