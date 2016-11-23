import sbt._, Keys._

object Dependencies {

  val ScalaVersions = Seq("2.11.8", "2.12.0")
  val AkkaVersion = "2.4.12"

  val Common = Seq(
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-stream"         % AkkaVersion,
      "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion   % Test,
      "org.scalatest"     %% "scalatest"           % "3.0.0"       % Test, // ApacheV2
      "com.novocode"       % "junit-interface"     % "0.11"        % Test, // BSD-style
      "junit"              % "junit"               % "4.12"        % Test  // Eclipse Public License 1.0
    )
  )
  
  val AkkaHttpVersion = "10.0.0-RC2"
  
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

}
