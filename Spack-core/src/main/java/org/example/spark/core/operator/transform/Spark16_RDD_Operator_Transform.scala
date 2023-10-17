package org.example.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark16_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 方法-groupByKey

    val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("b", 4)))

    //groupByKey:將我們的數據源中的數據，相同key的數據分在一個組中，形成一個對偶源組
    //元組中的第一個元素就是key，元組中的第2個元素就是相同key的value的集合
    /*
    spark中，shuffle操作是在硬碟處理，並不再內存裡面讓數據等待，會導致內存爆滿
    所以shuffle性能會比較低
     */
    val groupRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    groupRDD.collect().foreach(println)
    val groupRDD1: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(_._1)
    groupRDD1.collect().foreach(println)

    sc.stop();
  }
}
