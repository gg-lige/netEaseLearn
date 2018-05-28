package main.scala

import java.io.PrintWriter
import java.net.ServerSocket
import scala.io.Source

/**
  * Created by lg on 2018/5/28.
  *
  * 流数据模拟器
  * jar2
  */
//  查找到linux 下scala 的位置
//  /opt/wwd/scala/lib/scala-swing_2.11-1.0.2.jar /opt/wwd/scala/lib/scala-library.jar /opt/wwd/scala/lib/scala-actors-2.11.0.jar
//  java -cp /opt/lg/NetEase/jar2/netEaseLearn.jar main.scala.StreamingSimulation /opt/lg/minD.txt 9999 1000


object StreamingSimulation {
  //定义随机获取整数的方法
  def index(length: Int) = {
    import java.util.Random
    val rdm = new Random()
    rdm.nextInt(length)
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      System.err.println("Usage:<filename><port><millisecond>")
      System.exit(1)
    }

    //获取指定文件总行数
    val filename = args(0)
    val lines = Source.fromFile(filename).getLines().toList
    val filerow = lines.length

    //指定监听某端口，当外部程序请求时建立链接
    val listener = new ServerSocket(args(1).toInt)
    while (true) {
      val socket = listener.accept()
      new Thread() {
        override def run(): Unit = {
          println("Got client connected from : " + socket.getInetAddress)
          val out = new PrintWriter(socket.getOutputStream(), true)
          while (true) {
            Thread.sleep(args(2).toLong)
            //当该端口接受请求时，随机获取某行数据发送给对方
            val content = lines(index(filerow))
            println(content)
            out.write(content + '\n')
            out.flush()
          }
          socket.close()

        }

      }.start()
    }


  }


}
