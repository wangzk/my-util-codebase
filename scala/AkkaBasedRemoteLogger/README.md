# Akka-based Remote Logger

In a distributed environment, if there is a centralized easy-to-use log collector, it will help us collecting some metrics and tell us what is happening in the cluster.

## How to compile

`sbt package` to get the compiled jar,

or

`sbt assembly` to get the jar with dependencies.

## How to use

### Start log server
Before using the client, you need to run a log server first. Assume that you will start the log server on node `node1`.

Execute the following command in the terminal:

`scala -cp [path to the jar] wzk.akkalogger.server.AkkaLoggerServer`

Then the log server will be launched and start listen to the port 23587.


### Configuration
Edit the configuration file `akkalogger.conf` and change the `logger.server` field to the one that you actually run logger server on. The format is `server-host-name:23587`. In the example above, it will be `logger.server=node1:23587`.

### Use client in the program
Currently, it only supports a process-level logger. Import related classes and get the process-level client by `ProcessLevelClient.client`.

```scala
import wzk.akkalogger.client.{AkkaLoggerClient, ProcessLevelClient}
val client:AkkaLoggerClient = ProcessLevelClient.client
```

