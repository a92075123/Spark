package org.example.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark21_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    /*
   join 把相同的key的值組合再一起，
        如果兩個數據元中沒有相同的key不會出現在結果中
        如果兩個數據都多個相同的key的話，會多重匹配，因為數據會一直生長，效能會降低
     */

    val rdd = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)), 2)

    val rdd2 = sc.makeRDD(List(("a", 4), ("b", 5), ("c", 6)), 2)

    val value: RDD[(String, (Int, Int))] = rdd.join(rdd2)

    value.collect().foreach(println)


    sc.stop();
  }
}
