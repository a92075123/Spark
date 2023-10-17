package org.example.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark18_RDD_Operator_Transform3 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("b", 4), ("b", 5), ("a", 6)), 2)


//    rdd.aggregateByKey(0)(
//      (x, y) => (x + y),
//      (x, y) => (x + y)
//    ).collect().foreach(println)

    //最終的返回數據的結果跟初始值得類型保持一致
//    val value: RDD[(String, Int)] = rdd.foldByKey(0)(_ + _)
//    value.collect().foreach(println)

    //獲取相同key的數據的平均值=>(a,3),(b,4)
      //第一個0表示初始值，要對每個key的value做計算的初始值
      //第2個0表示，key出現的次數
    val value: RDD[(String, (Int, Int))] = rdd.aggregateByKey((0, 0))(
        /*
        t=t._1第一個0，t._2第2個0
        t._1總和，t._2key出現的次數
        v=每個key的value
         */
      (t, v) => {
        (t._1 + v, t._2 + 1)
      },
        /*
          把兩個分區互相 相加
          _1=總數和
          _2=key出現的次數和
         */
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )
    //mapValues指對值操作
    val value1: RDD[(String, Int)] = value.mapValues{
          /*
          num：這是與特定鍵（key）相關聯的所有值的和。
          cnt：這是與特定鍵（key）相關聯的值的數量。
           */
      case (num, cnt) => {
        num / cnt
      }
    }

    value1.collect().foreach(println)
    sc.stop();
  }
}
