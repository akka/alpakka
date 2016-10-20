lazy val amqp = (project in file(".")).
  configs(IntegrationTest).
  enablePlugins(AutomateHeaderPlugin)

name := "akka-stream-alpakka-amqp"

libraryDependencies ++= Seq(
  "com.rabbitmq" % "amqp-client"     % "3.6.1",           // APLv2
  "com.novocode" % "junit-interface" % "0.11"   % "test", // BSD-style
  "junit"        % "junit"           % "4.12"   % "test"  // Eclipse Public License 1.0
)

Defaults.itSettings
