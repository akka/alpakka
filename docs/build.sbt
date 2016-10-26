name := "Alpakka"

enablePlugins(ParadoxPlugin)
paradoxTheme := Some(builtinParadoxTheme("generic"))
paradoxProperties ++= Map(
  "version" -> version.value,
  "scala.binaryVersion" -> scalaBinaryVersion.value,
  "extref.akka-docs.base_url" -> s"http://doc.akka.io/docs/akka/${Common.AkkaVersion}/%s.html",
  "scaladoc.akka.base_url" -> s"http://doc.akka.io/api/akka/${Common.AkkaVersion}",
  "scaladoc.akka.stream.contrib.base_url" -> s"http://doc.akka.io/api/alpakka/${version.value}"
)

publishArtifact := false

apiURL := Some(url(s"http://doc.akka.io/api/alpakka/${version.value}"))
