package org.example.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark08_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 方法-sample 符合規則數據保留
    val rdd = sc.makeRDD(List(1,2,3,4,5,6,7,8,9,10))

    /*
    sample需要傳遞三個參數
    第一個參數 表示是否放回true(放回)false(丟棄)
    第二個參數 表示每一個數據被抽到的機率
      假設放回:代表會抽到重覆的並且每個數據還是一樣的機率會被抽到
      假設不放回:代表每個數據在執行時都會被分配一個隨機值介於0到1之間但不會是1，只要對應的隨機數小於fraction的值就會被選中
      並且每一次的執行後給於的分配值每次都會是不一樣
    第三個參數 抽到數據時隨機算法的種子
     如果沒值:不會出現固定結果
     如果給值:假設是1，會給你1種子固定的結果，假設是2，會給你2種子固定的結果，除非RDD或是fraction發生改變，不然的話結果永遠都是一樣的
     */
    println(rdd.sample(
      false,
      0.4
    ).collect().mkString(","))



    sc.stop();
  }
}
