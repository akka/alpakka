lazy val mqtt = (project in file(".")).
  enablePlugins(AutomateHeaderPlugin)

name := "akka-stream-contrib-mqtt"

libraryDependencies ++= Seq(
  "org.eclipse.paho" % "org.eclipse.paho.client.mqttv3" % "1.1.0",       // Eclipse Public License Version 1.0
  "io.moquette"      % "moquette-broker"                % "0.8.1" % Test // Apache License Version 2.0
)

resolvers += "moquette" at "http://dl.bintray.com/andsel/maven/"
