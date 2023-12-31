package org.example.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark17_RDD_Operator_Transform2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("b", 4), ("b", 5), ("a", 6)), 2)


    rdd.aggregateByKey(0)(
      (x, y) => (x + y),
      (x, y) => (x + y)
    ).collect().foreach(println)

    //如果聚合計算時，分區內和分區間的計算規則相同，spark提供了簡化的方法
    rdd.foldByKey(0)(_+_).collect().foreach(println)

    sc.stop();
  }
}
