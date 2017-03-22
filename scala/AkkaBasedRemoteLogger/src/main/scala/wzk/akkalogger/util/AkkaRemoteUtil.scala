package wzk.akkalogger.util

import java.net.InetAddress

import akka.actor.ActorSystem
import akka.io.Inet
import com.typesafe.config.ConfigFactory

object RemoteRelatedUtil {

  def remotingConfig(host:String, port:Int) = ConfigFactory.parseString(
    s"""
       |akka {
       |  actor.provider = "akka.remote.RemoteActorRefProvider"
       |  remote {
       |    enable-transports = ["akka.remote.netty.tcp"]
       |    netty.tcp {
       |      hostname = $host
       |      port = $port
       |    }
       |  }
       |}
     """.stripMargin)

  def remotingSystem(name: String, host: String, port: Int): ActorSystem =
    ActorSystem(name, remotingConfig(host, port))

  def createLocalSystem(name:String):ActorSystem =
    ActorSystem(name, remotingConfig(getLocalNodeHostname,  0)) // we only need random port for local system

  def getLocalNodeHostname:String = InetAddress.getLocalHost.getHostName
}