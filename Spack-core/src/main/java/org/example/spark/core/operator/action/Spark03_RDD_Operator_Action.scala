package org.example.spark.core.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operator_Action {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    //aggregateByKey:初始值只會在分區內計算
    //aggregate:會在分區內跟分區之間計算
    //fold:當分區間跟分區內的運算邏輯一樣的時候，可以使用，代表兩個都用同一個方法
    val i = rdd.aggregate(0)(_ + _, _ + _)

    println(i)


    sc.stop()
  }
}
