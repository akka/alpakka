lazy val alpakka = project
  .in(file("."))
  .enablePlugins(PublishUnidoc)
  .aggregate(amqp, cassandra, docs, files, mqtt, s3)

lazy val amqp = project
  .in(file("amqp"))
  .enablePlugins(AutomateHeaderPlugin)
  .settings(
    name := "akka-stream-alpakka-amqp",
    Dependencies.Amqp
  )

lazy val cassandra = project
  .in(file("cassandra"))
  .enablePlugins(AutomateHeaderPlugin)
  .settings(
    name := "akka-stream-alpakka-cassandra",
    Dependencies.Cassandra
  )

lazy val files = project
  .in(file("file"))
  .enablePlugins(AutomateHeaderPlugin)
  .settings(
    name := "akka-stream-alpakka-file",
    Dependencies.File
  )

lazy val mqtt = project
  .in(file("mqtt"))
  .enablePlugins(AutomateHeaderPlugin)
  .settings(
    name := "akka-stream-alpakka-mqtt",
    Dependencies.Mqtt,

    // Scala and Java tests start a separate MQTT broker.
    // Make it not step on each other by running Scala and Java tests sequentially.
    parallelExecution in Test := false
  )

lazy val s3 = project
  .in(file("s3"))
  .enablePlugins(AutomateHeaderPlugin)
  .settings(
    name := "akka-stream-alpakka-s3",
    Dependencies.S3
  )

lazy val docs = project
  .in(file("docs"))
  .enablePlugins(ParadoxPlugin, NoPublish)
  .disablePlugins(BintrayPlugin)
  .settings(
    name := "Alpakka",
    paradoxTheme := Some(builtinParadoxTheme("generic")),
    paradoxProperties ++= Map(
      "version" -> version.value,
      "scala.binaryVersion" -> scalaBinaryVersion.value,
      "extref.akka-docs.base_url" -> s"http://doc.akka.io/docs/akka/${Dependencies.AkkaVersion}/%s.html",
      "extref.java-api.base_url" -> "https://docs.oracle.com/javase/8/docs/api/index.html?%s.html",
      "extref.paho-api.base_url" -> "https://www.eclipse.org/paho/files/javadoc/index.html?%s.html",
      "scaladoc.akka.base_url" -> s"http://doc.akka.io/api/akka/${Dependencies.AkkaVersion}",
      "scaladoc.akka.stream.alpakka.base_url" -> s"http://doc.akka.io/api/alpakka/${version.value}"
    )
  )
