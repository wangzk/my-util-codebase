import wzk.akkalogger.client.{AkkaLoggerClient, ProcessLevelClient}
import scala.collection.immutable.HashMap

val prdd = sc.parallelize(0 to 99, 4)
prdd.count

prdd.foreach(v => {
 val client:AkkaLoggerClient = ProcessLevelClient.client
    client.logMetrics(HashMap[String,Long](("A", v), ("B", v+1), ("C", v+2)))
    client.logMetrics(HashMap[String,Long](("A", v), ("B", v+1), ("C", v+2)))
})
