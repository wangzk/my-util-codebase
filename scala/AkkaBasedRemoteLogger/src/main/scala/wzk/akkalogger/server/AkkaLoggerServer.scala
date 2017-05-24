package wzk.akkalogger.server

import java.io.PrintStream
import java.text.SimpleDateFormat

import akka.actor.Actor.Receive
import akka.actor.{ActorSystem, Props}
import akka.event.Logging
import akka.util.Timeout
import wzk.akkalogger.message.{AverageMetricLogMessage, ClearMetricLogMessage, SimpleStringMessage, WriteMetricLogMessage}
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
    System.err.println("Server started...")
    while (true) {
      System.err.print(">")
      val command = scala.io.StdIn.readLine()
      loggerService.sendCommand(command)
    }
  }
}


/**
  * The actor who actually response for the log messages from clients.
  */
class LoggerServerActor extends akka.actor.Actor {

  val log = Logging(context.system, this)

  // hash map to store metrics.
  // key: metric name
  // value: (metric count, metric value sum)
  val metrics:mutable.Map[String, (Long, Long)] = new mutable.HashMap[String, (Long, Long)]()
  val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

  def receive = {
    case AverageMetricLogMessage(metrics) => {
      for (metricInfo <- metrics) {
        val metric=  metricInfo._1
        val value = metricInfo._2
        logMetric(metric, value)
      }
      printCurrentAverageMetrics()
    }
    case SimpleStringMessage(senderHostName, msg) => {
      System.out.println(s"[MSG]${simpleDateFormat.format(new java.util.Date())} from ${senderHostName}: $msg")
      System.out.flush()
    }
    case ClearMetricLogMessage(msg) => {
      metrics.clear()
      System.out.println(s"Metrics cleared. ${msg}.")
      System.out.flush()
    }
    case WriteMetricLogMessage(fileName) => {
      val printStream = new PrintStream(fileName)
      printCurrentAverageMetrics(printStream)
      printStream.close()
    }
    case "reset" => {
      metrics.clear()
      System.out.flush()
    }
    case "info" => {
      printCurrentAverageMetrics()
    }
    case "stop" => {
      context.stop(self)
    }
  }


  private def logMetric(metricName:String, metricValue:Long) = {
    val metricInfo = metrics.getOrElse(metricName, (0L,0L))
    metrics.put(metricName, (metricInfo._1 + 1, metricInfo._2 + metricValue))
  }

  override def unhandled(message: Any): Unit = {
    super.unhandled(message)
    log.info(s"unhandled message: $message")
  }


  private[this] def printCurrentAverageMetrics(printStream: PrintStream = System.out): Unit = {
    printStream.println("\n===== Metrics Info =====")
    printStream.println(simpleDateFormat.format(new java.util.Date()))
    printStream.println("METRIC\tCOUNT\tSUM\tAVERAGE")
    val metricNames = metrics.keys.toSeq.sorted
    for (metric <- metricNames) {
      val (count, sum) = metrics(metric)
      printStream.println(s"${metric}\t${count}\t${sum}\t${"%.2f".format(sum/count.toDouble)}")
    }
    printStream.println("===== END =====")
    printStream.flush()
  }
}