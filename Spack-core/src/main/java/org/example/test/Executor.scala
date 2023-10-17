package org.example.test

import java.io.{InputStream, ObjectInput, ObjectInputStream}
import java.net.{ServerSocket, Socket}

object Executor {
  def main(args: Array[String]): Unit = {

    //啟動服務器，接收數據
    val server = new ServerSocket(9999)
    println("服務器啟動，等待接收數據")

    val client: Socket = server.accept()
    val in: InputStream = client.getInputStream
    val objIn = new ObjectInputStream(in)
    val task: SubTask = objIn.readObject().asInstanceOf[SubTask]
    val ints = task.compute()
    println("計算節點[9999]結果為" + ints)
    objIn.close()
    client.close()
    server.close()



  }
}
