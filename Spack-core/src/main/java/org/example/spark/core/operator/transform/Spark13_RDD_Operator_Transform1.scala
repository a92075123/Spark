package org.example.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark13_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 方法-雙Value類型
    //Can't zip RDDs with unequal numbers of partitions: List(2, 4)
    //兩個的數據源要求分區數量要保持一致
    //Can only zip RDDs with same number of elements in each partition
    //兩個數據源要求分區數量中數據要保持一致
    val rdd1 = sc.makeRDD(List(1, 2, 3, 4,5,6),2)
    val rdd2 = sc.makeRDD(List(3, 4, 5, 6),2)

    val rdd6: RDD[(Int, Int)] = rdd1.zip(rdd2)
    println(rdd6.collect().mkString(","))

    sc.stop();
  }
}
