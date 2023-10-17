package org.example.spark.core.operator.acc

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Acc {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("Persist")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    //reduce方區內的計算跟分區間的計算
//    val i: Int = rdd.reduce(_ + _)
//    println(i)
    var sum = 0
    rdd.foreach(
      num=>{
        sum+=num
      }
    )
    println(sum)
  }

}
