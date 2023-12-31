package org.example.spark.core.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_Operator_Action {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    val user = new User

    rdd.foreach(
      num=>{
        println("age  = "+ (user.age+num))
      }
    )


    sc.stop()
  }

  class User extends Serializable {
    var age:Int=30
  }


}
