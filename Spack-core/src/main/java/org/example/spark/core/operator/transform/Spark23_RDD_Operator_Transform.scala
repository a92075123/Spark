package org.example.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark23_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //跟sql一樣左連結跟右連結 

    val rdd = sc.makeRDD(List(("a", 1), ("b", 2)), 2)

    val rdd2 = sc.makeRDD(List(("a", 4), ("b", 5), ("c", 6),("c", 7)), 2)


    // 連結+分組:cogroup:connet+group

    val value: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd.cogroup(rdd2)


    value.collect().foreach(println)

    sc.stop();
  }
}
