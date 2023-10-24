package org.example.spark.core.operator.acc

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Acc {

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


    val mapRDD =rdd.map(
      num=>{
        //使用累加器
        sumAcc.add(num)
        num
      }
    )

    //獲取累加器的值
    //少加:主要是因為轉換算子(map)中調用累加器，如果沒有行動算子的話，不會執行累加
    //多加:主要是因為轉換算子(map)中調用累加器，如果沒有行動算子的話，不會執行累加
    //一班情況下，累加器會放置在行動算子進行操作

    mapRDD.collect()
    mapRDD.collect()
    println(sumAcc.value)

    sc.stop()
  }

}
