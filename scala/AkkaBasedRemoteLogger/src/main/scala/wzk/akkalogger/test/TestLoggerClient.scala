package wzk.akkalogger.test

import wzk.akkalogger.client.{AkkaLoggerClient, ProcessLevelClient}

import scala.collection.immutable.HashMap

/**
  * Created by wangzhaokang on 3/22/17.
  */
object TestLoggerClient {
  def main(args:Array[String]) = {
    val client:AkkaLoggerClient = ProcessLevelClient.client
    client.logMetrics(HashMap[String,Long](("A", 1), ("B", 2), ("C", 3)))
    Thread.sleep(1000)
    client.logMetrics(HashMap[String,Long](("A", 2), ("B", 2), ("C", 4)))
    Thread.sleep(1000)
    client.logMetrics(HashMap[String,Long](("A", 3), ("B", 2), ("C", 5)))
    Thread.sleep(1000)

    System.exit(0)
  }

}
