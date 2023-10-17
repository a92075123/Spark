package org.example.spark.core.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark20_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("b", 4), ("b", 5), ("a", 6)), 2)

    rdd.reduceByKey(_+_)//wordCount

    //(a,[1,2]),(a,[3,4])
    //aggregateByKey存在函數柯里化，有兩個參數列表
    //    第一個參數列表，需要傳第一個參數，表示為初始值
    //    主要用於當碰見第一個key的時候，和value進行分區內計算
    //第二個參數列表需要傳遞2個參數
    //    第一個參數表示分區內的計算規則
    //    第二個參數表示分區間計算的規則
    //math.max(x,y)取他們哪一個大的值
    rdd.aggregateByKey(0)(_+_,_+_)//wordCount

    //如果聚合計算時，分區內和分區間的計算規則相同，spark提供了簡化的方法
    rdd.foldByKey(0)(_+_)//wordCount
    /*
    combineByKey:方法需要三個參數
      第一個參數:將相同key的第一個數據，進行結構的轉換，並且來做業務邏輯
      第二個參數:分區內的計算規則
      第三個參數:分區間的計算規則
     */
    //這個函數會用該鍵的第一個值來創建一個新的 (Int, Int) 。
    //例如，如果鍵 "apple" 的第一個值是 3，那麼該對就會是 (3, 1)。

    //(x: Int, y) => x + y: 在 mergeValue 階段，這表示當在同一個分區內再次遇到相同的鍵時
    // 它會將當前合併器（已經存儲的值） x 和新值 y 加在一起。這就是一個簡單的加法運算。

    //(x: Int, y: Int) => x + y: 在 mergeCombiners 階段，如果多個分區有相同的鍵
    // ，這個函數會被用來合併這些分區的合併器。這裏也是簡單的加法運算，它會將兩個分區的合併器值加在一起。
    rdd.combineByKey(v=>v,(x:Int,y)=>x+y,(x:Int,y:Int)=>x+y)//wordCount






    sc.stop();
  }
}
