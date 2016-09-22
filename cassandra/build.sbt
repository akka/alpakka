lazy val cassandra = (project in file(".")).
  configs(IntegrationTest).
  enablePlugins(AutomateHeaderPlugin)

name := "akka-stream-contrib-cassandra"

libraryDependencies ++= Seq(
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.1.0"
)

Defaults.itSettings
