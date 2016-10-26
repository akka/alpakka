lazy val alpakka = project
  .in(file("."))
  .aggregate(amqp, cassandra, docs, mqtt)
  .settings(
    publishArtifact := false,
    unidocSettings
  )

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

lazy val mqtt = project
  .in(file("mqtt"))
  .enablePlugins()
  .settings(
    name := "akka-stream-alpakka-mqtt",
    Dependencies.Mqtt
  )

lazy val docs = project
  .in(file("docs"))
  .enablePlugins(ParadoxPlugin)
  .settings(
    name := "Alpakka",
    paradoxTheme := Some(builtinParadoxTheme("generic")),
    paradoxProperties ++= Map(
      "version" -> version.value,
      "scala.binaryVersion" -> scalaBinaryVersion.value,
      "extref.akka-docs.base_url" -> s"http://doc.akka.io/docs/akka/${Dependencies.AkkaVersion}/%s.html",
      "scaladoc.akka.base_url" -> s"http://doc.akka.io/api/akka/${Dependencies.AkkaVersion}",
      "scaladoc.akka.stream.contrib.base_url" -> s"http://doc.akka.io/api/alpakka/${version.value}"
    ),
    publishArtifact := false
  )
