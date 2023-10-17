package org.example.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 方法

    val rdd = sc.makeRDD(List(1, 2, 3, 4))
    //1,2,3,4
    //2,4,6,8

    //方法1 轉換函數
    def mapFunction(num:Int):Int={
      num*2
    }
//    val mapRDD: RDD[Int] = rdd.map(mapFunction)

    //方法2
//    val mapRDD: RDD[Int] = rdd.map((num:Int)=>{
//      num*2
//    })
    //方法3 當邏輯只有一行的時候，可以省略大括號
//    val mapRDD: RDD[Int] = rdd.map((num: Int) => num * 2)
    //當類型可以自動推算出來，可以省略Int
//    val mapRDD: RDD[Int] = rdd.map((num) => num * 2)
    //當需求函數只有一個，可以省略小括號
//    val mapRDD: RDD[Int] = rdd.map(num => num * 2)

    //當需求函數只出現一次還有他是按造順序出現可以使用_
    val mapRDD: RDD[Int] = rdd.map(_*2)

    mapRDD.collect().foreach(println)

    sc.stop();
  }
}
