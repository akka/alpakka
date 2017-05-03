lazy val xmlparser = (project in file(".")).
  enablePlugins(AutomateHeaderPlugin)

name := "akka-stream-contrib-xmlparser"

libraryDependencies ++= Seq(
  "com.fasterxml" % "aalto-xml" % "1.0.0"       // Apache License 2.0
)

