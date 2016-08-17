lazy val root = (project in file(".")).
  aggregate(contrib, mqtt)

lazy val contrib = project
lazy val mqtt = project
lazy val amqp = project
