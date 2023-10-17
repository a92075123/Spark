package org.example.spark.core.operator.acc

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Acc {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("Persist")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    //獲取系統累加器
    //spark默認提供簡單數據聚合的累加器
    val sumAcc = sc.longAccumulator("sum")
//    以下都是累加器
//    sc.doubleAccumulator()
//    sc.collectionAccumulator()


    rdd.foreach(
      num=>{
        //使用累加器
        sumAcc.add(num)
      }
    )
    //獲取累加器的值
    println(sumAcc.value)

    sc.stop()
  }

}
