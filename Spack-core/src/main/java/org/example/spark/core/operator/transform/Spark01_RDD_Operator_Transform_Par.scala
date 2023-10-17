package org.example.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform_Par {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 方法
    //1.rdd的計算一個分區內的數據是一個一個執行
    //只有前面一個數據全部的邏輯執行完畢後，才會執行下一個
    //分區內的數據執行是有序的
    val rdd = sc.makeRDD(List(1,2,3,4),2)

    val mapRDD = rdd.map(num => {
      println(">>>>>>>"+num)
      num
    })
    val mapRDD1 = mapRDD.map(num => {
      println("#######"+num)
      num
    })

    mapRDD1.collect()


    sc.stop();
  }
}
