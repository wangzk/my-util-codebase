package wzk.akkalogger.message


/**
  * Message type that carries metrics that need to be averaged on the logger side.
  */
case class AverageMetricLogMessage(val metrics:Map[String, Long]) {}

case class SimpleStringMessage(val senderHostName:String, val msg:String) {}
