package org.example.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 方法-Partitions
    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)

    //[1,2][3,4]
    //[2],[4]


    //因為mapPartitions的返回類型是iterator
    //所以直接把存成list在轉換iterator就好了
    val mpRDD = rdd.mapPartitions(item => {
      List(item.max).iterator
    })

    mpRDD.collect().foreach(println)
    sc.stop();
  }
}
