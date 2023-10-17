package org.example.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark17_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 方法-aggregateByKey 可做出分區間與分區內，兩個不同的邏輯，這裡是先取分區內誰比較大，再把分區與分區間的最大值相加

    val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4)),2)

    //(a,[1,2]),(a,[3,4])
    //aggregateByKey存在函數柯里化，有兩個參數列表
    //    第一個參數列表，需要船第一個參數，表示為初始值
    //    主要用於當碰見第一個key的時候，和value進行分區內計算
    //第二個參數列表需要傳遞2個參數
    //    第一個參數表示分區內的計算規則
    //    第二個參數表示分區間計算的規則
    //math.max(x,y)取他們哪一個大的值
    rdd.aggregateByKey(0)(
      (x,y)=>math.max(x,y),
      (x,y)=>(x+y)
    ).collect().foreach(println)



    sc.stop();
  }
}
