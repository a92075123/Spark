package org.example.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark19_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("b", 4), ("b", 5), ("a", 6)), 2)


    /*
    combineByKey:方法需要三個參數
      第一個參數:將相同key的第一個數據，進行結構的轉換，並且來做業務邏輯
      第二個參數:分區內的計算規則
      第三個參數:分區間的計算規則
     */
    val value: RDD[(String, (Int, Int))] = rdd.combineByKey(
      //這個函數會用該鍵的第一個值來創建一個新的 (Int, Int) 。
      //例如，如果鍵 "apple" 的第一個值是 3，那麼該對就會是 (3, 1)。
      v =>(v,1),
      (t:(Int,Int),v)=>{
        (t._1 + v, t._2 + 1)
      },

      (t1:(Int,Int), t2:(Int,Int)) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )

    val value1: RDD[(String, Int)] = value.mapValues {

      case (num, cnt) => {
        num / cnt
      }
    }

    value1.collect().foreach(println)
    sc.stop();
  }
}
