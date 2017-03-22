package wzk.akkalogger.client

/**
  * A singleton that carries the process level client
  */
object ProcessLevelClient {
  val client:AkkaLoggerClient = new AkkaLoggerClient()
}
