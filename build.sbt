lazy val root = (project in file(".")).
  aggregate(contrib, mqtt, amqp, xmlparser, cassandra).
  enablePlugins(GitVersioning)

lazy val contrib = project
lazy val mqtt = project
lazy val amqp = project
lazy val xmlparser = project
lazy val cassandra = project

git.useGitDescribe := true
publishArtifact := false
