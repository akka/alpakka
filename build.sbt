lazy val root = (project in file(".")).
  aggregate(amqp, cassandra, docs, mqtt)

lazy val amqp = project
lazy val cassandra = project
lazy val docs = project
lazy val mqtt = project

publishArtifact := false
unidocSettings
