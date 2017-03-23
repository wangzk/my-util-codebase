package wzk.akkalogger.server

import java.text.SimpleDateFormat

import akka.actor.Actor.Receive
import akka.actor.{ActorSystem, Props}
import akka.event.Logging
import akka.util.Timeout
import wzk.akkalogger.message.{AverageMetricLogMessage, SimpleStringMessage}
import wzk.akkalogger.util.RemoteRelatedUtil

import scala.collection.mutable
import scala.concurrent.duration.{DAYS, FiniteDuration}

/**
  * Logger server
  */
class AkkaLoggerServer {

  val debugLocalLogger = java.util.logging.Logger.getLogger(this.getClass.getName)
  var system:ActorSystem = null
  var loggerServer: akka.actor.ActorRef = null
  implicit val timeout = new Timeout(FiniteDuration(1, DAYS))

  loadActorSystem()

  private[this] def loadActorSystem(): Unit = {
    system = RemoteRelatedUtil.remotingSystem(
      "LoggerServer",
      RemoteRelatedUtil.getLocalNodeHostname,
      AkkaLoggerServer.SERVER_PORT)
    loggerServer = system.actorOf(Props[LoggerServerActor], "server")
  }

  def sendCommand(command: String): Unit = {
    loggerServer ! command
  }


}

object AkkaLoggerServer {
  val SERVER_PORT = 23587

  def main(args:Array[String]): Unit = {
    val loggerService = new AkkaLoggerServer
    println("Server started...")
    while (true) {
      print(">")
      val command = scala.io.StdIn.readLine()
      loggerService.sendCommand(command)
    }
  }
}


class LoggerServerActor extends akka.actor.Actor {

  val log = Logging(context.system, this)
  val metricsNeedAverage:mutable.Map[String, Long] = new mutable.HashMap[String, Long]()
  var metricLogCount = 0L
  val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd- HH:mm:ss.SSS")

  def receive = {
    case AverageMetricLogMessage(metrics) => {
      metricLogCount = metricLogCount + 1
      for (metricInfo <- metrics) {
        val metric=  metricInfo._1
        val value = metricInfo._2
        metricsNeedAverage(metric) =
          metricsNeedAverage.getOrElse(metric, 0L) + value
      }
      printCurrentAverageMetrics()
    }
    case SimpleStringMessage(msg) => {
      System.out.println(s"[MSG]${simpleDateFormat.format(new java.util.Date())}${sender}: $msg")
      System.out.flush()
    }
    case "reset" => {
      metricsNeedAverage.clear()
      metricLogCount = 0
    }
    case "info" => {
      printCurrentAverageMetrics()
    }
    case "stop" => {
      context.stop(self)
    }
  }

  override def unhandled(message: Any): Unit = {
    super.unhandled(message)
    log.info(s"unhandled message: $message")
  }


  private[this] def printCurrentAverageMetrics(): Unit = {
    println("\n===== Current Average Metrics =====")
    println(simpleDateFormat.format(new java.util.Date()))
    println("METRIC\t\tCOUNT\t\tAVERAGE")
    for ((metric, value) <- metricsNeedAverage) {
      printf("%s\t\t%d\t\t%.2f\n", metric, metricLogCount, value / metricLogCount.toDouble)
    }
    println("===== DONE =====")
  }



}