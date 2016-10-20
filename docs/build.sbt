name := "Alpakka"

enablePlugins(ParadoxPlugin)
paradoxTheme := Some(builtinParadoxTheme("generic"))
paradoxProperties ++= Map(
  "version" -> version.value,
  "scala.binaryVersion" -> scalaBinaryVersion.value
)

publishArtifact := false
