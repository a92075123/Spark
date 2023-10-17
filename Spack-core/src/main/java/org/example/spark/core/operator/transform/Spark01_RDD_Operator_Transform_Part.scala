package org.example.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform_Part {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 方法
    //每個分區都是獨立的，所計算出來的數據，只會在自己的分區
    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)
    //[1,2][3,4]
    rdd.saveAsTextFile("output")
    val mapRDD = rdd.map(_ * 2)
    //[2,4],[6,8]
    mapRDD.saveAsTextFile("output1")

    sc.stop();
  }
}
