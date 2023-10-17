package org.example.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark09_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 方法-distinct 去重複
    val rdd = sc.makeRDD(List(1, 2, 3, 4,1,2,3,4))

    val rdd1: RDD[Int] = rdd.distinct()

    //map(x=>(x,null).reduceByKey((x,_)=>x,numPartitions).map(_._1)
    //每個數值都給他一個value:null，並且只取得第一個值
    //假設(1,null)(1,null)(1,null)
    //只會取第一個(1,null)
    //然後保留1去掉null
    rdd1.collect().foreach(println)


    sc.stop();
  }
}
