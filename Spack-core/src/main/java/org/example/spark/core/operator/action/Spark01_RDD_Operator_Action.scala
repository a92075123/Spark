package org.example.spark.core.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Action {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1,2,3,4))

    //運算子
    //所謂的執行運算子，就是觸發作業(job)執行的方法
    //底層代碼用的是環境對象的是runJob方法
    //底層代碼中會創建activejob,並提交執行
    rdd.collect()


  }
}
