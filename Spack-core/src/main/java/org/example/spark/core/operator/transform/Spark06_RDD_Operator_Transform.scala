package org.example.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 方法-groupBy
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4,5,6), 2)

    //groupBy會將數據源中的每一個數據進行分組判斷，根據返回的分組key進行分組
    //相同的key值得數據會放置在一個組中
    def groupFunction(num:Int)={
      num%2
    }

    val groupRDD: RDD[(Int, Iterable[Int])] = rdd.groupBy(groupFunction)

    groupRDD.collect().foreach(println)

    sc.stop();
  }
}
