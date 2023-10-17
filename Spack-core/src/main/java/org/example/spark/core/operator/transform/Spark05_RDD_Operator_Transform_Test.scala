package org.example.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 方法-glom
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    //求分區最大值，再把分區的最大值相加
    //[1,2],[3,4]
    //[2],[4]
    //[6]
    val glomRDD: RDD[Array[Int]] = rdd.glom
    val maxRDD = glomRDD.map(
      array => {
        array.max
      }
    )
    println(maxRDD.collect().sum)


    sc.stop();
  }
}
