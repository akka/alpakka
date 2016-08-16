lazy val root = (project in file(".")).
  aggregate(contrib, mqtt)

lazy val contrib = project.
  enablePlugins(AutomateHeaderPlugin)

lazy val mqtt = project.
  enablePlugins(AutomateHeaderPlugin)

lazy val amqp = project
