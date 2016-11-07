lazy val alpakka = project
  .in(file("."))
  .enablePlugins(NoPublish, DeployRsync)
  .disablePlugins(BintrayPlugin)
  .aggregate(amqp, cassandra, docs, mqtt)
  .settings(
    unidocSettings,
    deployRsyncArtifact := (sbtunidoc.Plugin.UnidocKeys.unidoc in Compile).value.head -> s"www/api/alpakka/${version.value}"
  )

lazy val amqp = project
  .in(file("amqp"))
  .enablePlugins(AutomateHeaderPlugin)
  .settings(
    name := "akka-stream-alpakka-amqp",
    Dependencies.Amqp
  )

lazy val `akka-stream-alpakka-file` = project
  .in(file("file"))
  .enablePlugins(AutomateHeaderPlugin)
  .settings(
    Dependencies.File
  )

lazy val cassandra = project
  .in(file("cassandra"))
  .enablePlugins(AutomateHeaderPlugin)
  .settings(
    name := "akka-stream-alpakka-cassandra",
    Dependencies.Cassandra
  )

lazy val mqtt = project
  .in(file("mqtt"))
  .enablePlugins()
  .settings(
    name := "akka-stream-alpakka-mqtt",
    Dependencies.Mqtt,

    // Scala and Java tests start a separate MQTT broker.
    // Make it not step on each other by running Scala and Java tests sequentially.
    parallelExecution in Test := false
  )

lazy val docs = project
  .in(file("docs"))
  .enablePlugins(ParadoxPlugin, NoPublish, DeployRsync)
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
    ),
    deployRsyncArtifact := (paradox in Compile).value -> s"www/docs/alpakka/${version.value}"
  )
