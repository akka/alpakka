import sbt._, Keys._

object Dependencies {

  val ScalaVersions = Seq("2.11.8", "2.12.0")
  val AkkaVersion = "2.4.14"

  val Common = Seq(
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-stream"         % AkkaVersion,
      "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion   % Test,
      "org.scalatest"     %% "scalatest"           % "3.0.0"       % Test, // ApacheV2
      "com.novocode"       % "junit-interface"     % "0.11"        % Test, // BSD-style
      "junit"              % "junit"               % "4.12"        % Test  // Eclipse Public License 1.0
    )
  )

  val AkkaHttpVersion = "10.0.0"

  val S3 = Seq(
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-http"     % AkkaHttpVersion,
      "com.typesafe.akka" %% "akka-http-xml" % AkkaHttpVersion,
      // in-memory filesystem for file related tests
      "com.google.jimfs"   % "jimfs" % "1.1" % Test // ApacheV2
    )
  )

  val Amqp = Seq(
    libraryDependencies ++= Seq(
      "com.rabbitmq" % "amqp-client" % "3.6.1" // APLv2
    )
  )

  val Cassandra = Seq(
    libraryDependencies ++= Seq(
      "com.datastax.cassandra" % "cassandra-driver-core" % "3.1.0" // ApacheV2
    )
  )

  val HBase = {
    val hbaseVersion = "1.2.4"
    val hadoopVersion = "2.5.1"
    Seq(
      libraryDependencies ++= Seq(
        "org.apache.hbase" % "hbase-client" % hbaseVersion exclude("log4j", "log4j") exclude("org.slf4j", "slf4j-log4j12"), // ApacheV2,
        "org.apache.hbase" % "hbase-common" % hbaseVersion exclude("log4j", "log4j") exclude("org.slf4j", "slf4j-log4j12"), // ApacheV2,
        "org.apache.hadoop" % "hadoop-common" % hadoopVersion exclude("log4j", "log4j") exclude("org.slf4j", "slf4j-log4j12"), // ApacheV2,
        "org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVersion exclude("log4j", "log4j") exclude("org.slf4j", "slf4j-log4j12"), // ApacheV2,
        "org.slf4j" % "log4j-over-slf4j" % "1.7.21" % Test, // MIT like: http://www.slf4j.org/license.html
        "org.slf4j" % "slf4j-api" % "1.7.21" % Test, // MIT like: http://www.slf4j.org/license.html
        "ch.qos.logback" % "logback-classic" % "1.1.7" % Test, // Eclipse Public License 1.0: http://logback.qos.ch/license.html
        "ch.qos.logback" % "logback-core" % "1.1.7" % Test // Eclipse Public License 1.0: http://logback.qos.ch/license.html
      )
    )
  }

  val Mqtt = Seq(
    libraryDependencies ++= Seq(
      "org.eclipse.paho" % "org.eclipse.paho.client.mqttv3" % "1.1.0",       // Eclipse Public License 1.0
      "io.moquette"      % "moquette-broker"                % "0.8.1" % Test // ApacheV2
    ),
    resolvers += "moquette" at "http://dl.bintray.com/andsel/maven/"
  )

  val File = Seq(
    libraryDependencies ++= Seq(
      "com.google.jimfs"  %  "jimfs"               % "1.1"  % Test  // ApacheV2
    )
  )

  val Ftp = Seq(
    libraryDependencies ++= Seq(
      "commons-net"           % "commons-net"          % "3.5",             // ApacheV2
      "com.jcraft"            % "jsch"                 % "0.1.54",          // BSD-style
      "org.apache.ftpserver"  % "ftpserver-core"       % "1.0.6"    % Test, // ApacheV2
      "org.apache.sshd"       % "sshd-core"            % "1.3.0"    % Test, // ApacheV2
      "com.google.jimfs"      % "jimfs"                % "1.1"      % Test, // ApacheV2
      "org.slf4j"             % "slf4j-api"            % "1.7.21"   % Test, // MIT
      "ch.qos.logback"        % "logback-classic"      % "1.1.7"    % Test, // Eclipse Public License 1.0
      "ch.qos.logback"        % "logback-core"         % "1.1.7"    % Test  // Eclipse Public License 1.0
    )
  )

  val Sqs = Seq(
    libraryDependencies ++= Seq(
      "com.amazonaws"   % "aws-java-sdk-sqs"    % "1.11.51" // ApacheV2
    )
  )
}
