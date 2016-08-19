lazy val root = (project in file(".")).
  aggregate(contrib, mqtt, amqp).
  enablePlugins(GitVersioning)

lazy val contrib = project
lazy val mqtt = project
lazy val amqp = project

git.useGitDescribe := true
publishArtifact := false
