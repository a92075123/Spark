package org.example.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark15_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 方法-reduceByKey類型
    //跟groupByKey相比，reduceByKey可以事先聚合讓他在硬碟得時候不會有太多資料影響到效能

    val rdd = sc.makeRDD(List(("a",1),("a",2),("a",3),("b",4)))

    //相同key的數據進行value的聚合的操作
    //scala語言中一般的聚合操作是兩兩聚合，spark基於scala開發的，所以她的聚合也是兩兩聚合
    //[1,2,3]
    //[3,3]
    //[6]
    //reduceByKey中如果key的數據只有一個，是不會參與運算的
    val reduceRDD = rdd.reduceByKey((x: Int, y: Int) => {
      x + y
    })
    reduceRDD.collect().foreach(println)


    sc.stop();
  }
}
