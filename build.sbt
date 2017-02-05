lazy val alpakka = project
  .in(file("."))
  .enablePlugins(PublishUnidoc)
  .aggregate(amqp, cassandra, dynamodb, docs, files, ftp, hbase, jms, mqtt, s3, simpleCodecs, sqs, sse)

lazy val amqp = project
  .enablePlugins(AutomateHeaderPlugin)
  .settings(
    name := "akka-stream-alpakka-amqp",
    Dependencies.Amqp
  )

lazy val cassandra = project
  .enablePlugins(AutomateHeaderPlugin)
  .settings(
    name := "akka-stream-alpakka-cassandra",
    Dependencies.Cassandra
  )

lazy val dynamodb = project
  .enablePlugins(AutomateHeaderPlugin)
  .settings(
    name := "akka-stream-alpakka-dynamodb",
    Dependencies.DynamoDB
  )

lazy val files = project // The name file is taken by `sbt.file`!
  .in(file("file"))
  .enablePlugins(AutomateHeaderPlugin)
  .settings(
    name := "akka-stream-alpakka-file",
    Dependencies.File
  )

lazy val ftp = project
  .enablePlugins(AutomateHeaderPlugin)
  .settings(
    name := "akka-stream-alpakka-ftp",
    Dependencies.Ftp,
    parallelExecution in Test := false
  )

lazy val hbase = project
  .enablePlugins(AutomateHeaderPlugin)
  .settings(
    name := "akka-stream-alpakka-hbase",
    Dependencies.HBase,
    fork in Test := true
  )

lazy val jms = project
  .enablePlugins(AutomateHeaderPlugin)
  .settings(
    name := "akka-stream-alpakka-jms",
    Dependencies.Jms,
    parallelExecution in Test := false
  )

lazy val mqtt = project
  .enablePlugins(AutomateHeaderPlugin)
  .settings(
    name := "akka-stream-alpakka-mqtt",
    Dependencies.Mqtt
  )

lazy val s3 = project
  .enablePlugins(AutomateHeaderPlugin)
  .settings(
    name := "akka-stream-alpakka-s3",
    Dependencies.S3
  )

lazy val simpleCodecs = project
  .in(file("simple-codecs"))
  .enablePlugins(AutomateHeaderPlugin)
  .settings(
    name := "akka-stream-alpakka-simple-codecs"
  )

lazy val sqs = project
  .in(file("sqs"))
  .enablePlugins(AutomateHeaderPlugin)
  .settings(
    name := "akka-stream-alpakka-sqs",
    Dependencies.Sqs
  )

lazy val sse = project
  .enablePlugins(AutomateHeaderPlugin)
  .settings(
    name := "akka-stream-alpakka-sse",
    Dependencies.Sse
  )

lazy val docs = project
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
      "scaladoc.akka.stream.alpakka.base_url" -> s"http://developer.lightbend.com/docs/api/alpakka/${version.value}"
    )
  )
