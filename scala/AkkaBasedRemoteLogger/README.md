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
Edit the configuration file `akkalogger.conf` and change the `logger.server` field to the one that you actually run logger server on. The format is `server-host-name:23587`. In the example above, it will be 

`logger.server=node1:23587`

in the configuration file.

### Use client in the program

#### Get the log client
Currently, it only supports a process-level logger. Import related classes and get the process-level client by `ProcessLevelClient.client`.

```scala
import wzk.akkalogger.client.{AkkaLoggerClient, ProcessLevelClient}
val client:AkkaLoggerClient = ProcessLevelClient.client
```

#### Log a metric
The `logMetrics` function of `AkkaLoggerClient` will send metrics to the server. Metrics will be automatically averaged on the server side with metrics got previously.

```scala
    client.logMetrics(HashMap[String,Long](("A", 1), ("B", 2), ("C", 3)))
```

#### Log a simple string
The `log` function of `AkkaLoggerClient` will send a simple string log to the server and display on the server side.

### Control the log server
When the log server is started, it will be in a little shell mode and wait for the user to send some commands to it.

The supported command list:
1. `info`, show the current metrics information;
2. `reset`, reset the metrics;
3. `stop`, stop the logger server.