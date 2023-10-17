package org.example.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 方法-filter 符合規則數據保留
    val rdd = sc.textFile("datas/apache.log")


    //TODO 只取得17/05/2015的數據
    rdd.filter(
      line=>{
        val datas =line.split(" ")
        val time =datas(3)
        time.startsWith("17/05/2015")
      }
    ).collect().foreach(println)

    sc.stop();
  }
}
