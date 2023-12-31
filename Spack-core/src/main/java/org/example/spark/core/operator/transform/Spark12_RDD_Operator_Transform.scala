package org.example.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark12_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 方法-sortBy 根據規則排序
    val rdd = sc.makeRDD(List(6, 2, 4, 4, 3, 1), 2)

    val newRDD: RDD[Int] = rdd.sortBy(num => num)

    newRDD.saveAsTextFile("output")

    sc.stop();
  }
}
