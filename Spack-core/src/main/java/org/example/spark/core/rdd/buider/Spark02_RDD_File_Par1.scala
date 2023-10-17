package org.example.spark.core.rdd.buider

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File_Par1 {
  def main(args: Array[String]): Unit = {
    //TODO 準備環境
    //local[*]=你有幾核心就用幾核
    //setAppName幫應用程序取名字
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 創建RDD
    /*
     1.數據以行為單位進行讀取
       spark讀取文件是採用hadoop的方式，所以一行一行讀取，跟字節沒關西
     2.數據讀取時以偏移量為單位，偏移量不會被重複讀取
     1@@(換行符號) =>012篇移量
     2@@=> 345
     3@@=> 6

     3.數據分區的偏移量範圍計算
      0=>[0,3]
      1=>[3,6]
      2=>[6,7]
     */
    val rdd = sc.textFile("datas/1.txt",2)
    rdd.saveAsTextFile("output")




    //TODO 關閉環境
    sc.stop()
  }
}
