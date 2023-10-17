package org.example.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 方法-Partitions
    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)

    //[1,2] [3,4]
    //以分區為單位，可以選擇要哪一個分區(index)
    val mpiRDD = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        if (index == 1) {
          iter
        } else {
          //Nil=空集合
          Nil.iterator
        }
      }
    )
    mpiRDD.collect().foreach(println)

    sc.stop();
  }
}
