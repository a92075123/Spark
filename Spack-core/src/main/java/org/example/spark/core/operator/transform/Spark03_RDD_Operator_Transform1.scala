package org.example.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 方法-Partitions
    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    //取得數據的分區號碼
    val mpiRDD = rdd.mapPartitionsWithIndex((index, iter) => {
      // 1 ,2 , 3 , 4
      //(0,1)(2,2)(4,3)(6,4)
      iter.map(
        num => {
          (index, num)
        }
      )
    })

    mpiRDD.collect().foreach(println)


    sc.stop();
  }
}
