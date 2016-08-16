lazy val amqp = (project in file(".")).
  configs(IntegrationTest)
  .enablePlugins(AutomateHeaderPlugin)

name := "akka-stream-contrib-amqp"

libraryDependencies ++= Seq(
  "com.rabbitmq"      %	 "amqp-client"           % "3.6.1" // APLv2
)

Defaults.itSettings