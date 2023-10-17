package org.example.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark14_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 方法-Key-Value類型

    val rdd= sc.makeRDD(List(1,2,3,4))
    val mapRDD :RDD[(Int,Int)]= rdd.map((_,1));
    //RDD=>PairRDDFunctions
    //隱式轉換(2次編譯)

    //partitionBy根據指定的分區規則對數據進行重分區
    mapRDD.partitionBy(new HashPartitioner(2))
      .saveAsTextFile("output")


    sc.stop();
  }
}
