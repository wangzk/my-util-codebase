name := "AkkaBasedRemoteLogger"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.3.10"

libraryDependencies += "com.typesafe.akka" %% "akka-remote" % "2.3.10"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)


