package wzk.akkalogger.client

import java.io.File
import java.util.Properties

import akka.actor.ActorSystem
import akka.util.Timeout
import wzk.akkalogger.message.{AverageMetricLogMessage, SimpleStringMessage}
import wzk.akkalogger.util.RemoteRelatedUtil

import scala.concurrent.Await
import scala.concurrent.duration.{DAYS, FiniteDuration}
import scala.io.Source


/**
  * Akka logger client.
  */
class AkkaLoggerClient(private var configFilePath:String = "akkalogger.conf") {

  private val debugLocalLogger = java.util.logging.Logger.getLogger(this.getClass.getName)
  private var remoteAkkaSystem:String = ""
  private var loggerPath:String = ""
  private var system:ActorSystem = null
  private var loggerServer: akka.actor.ActorRef = null
  private implicit val timeout = new Timeout(FiniteDuration(1, DAYS))

  loadConfigure()
  loadActorSystem()

  def this(configFile:File) {
    this()
    configFilePath = configFile.getAbsolutePath
    loadConfigure()
    loadActorSystem()
  }

  private[this] def loadConfigure(): Unit = {
    val prop: Properties = new Properties()
    prop.load(Source.fromFile(configFilePath).reader())
    remoteAkkaSystem = prop.getProperty("logger.server")
  }

  private[this] def loadActorSystem(): Unit = {
    val serverName = remoteAkkaSystem.split(":")(0)
    val port = remoteAkkaSystem.split(":")(1).toInt
    system = RemoteRelatedUtil.createLocalSystem("LoggerClient")
    loggerServer = Await.result(
      system.actorSelection(s"akka.tcp://LoggerServer@$serverName:$port/user/server").resolveOne,
      timeout.duration)
    debugLocalLogger.info(s"Successfully get logger server ActorRef:${loggerServer}.")
  }

  private[this] def checkServerAvailable():Unit = {
    if (loggerServer == null) {
      debugLocalLogger.severe("try to use a un-initialized log service.")
      throw new Exception("server not initialized")
    }
  }

  /**
    * Send metrics to the server and the metrics will be automatically averaged on the server side.
    * @param metrics Metrics. It is a map (metric:String -> value:Long).
    */
  def logMetrics(metrics:Map[String, Long]):Unit = {
    checkServerAvailable()
    loggerServer ! AverageMetricLogMessage(metrics)
  }

  def log(msg:String): Unit = {
    checkServerAvailable()
    loggerServer ! SimpleStringMessage(msg)
  }
}

