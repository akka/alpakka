addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.4.3")
addSbtPlugin("com.dwijnand" % "sbt-dynver" % "4.1.1")
addSbtPlugin("com.geirsson" % "sbt-ci-release" % "1.5.7")
// discipline
addSbtPlugin("de.heikoseeberger" % "sbt-header" % "5.7.0")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.6")
addSbtPlugin("com.typesafe" % "sbt-mima-plugin" % "1.1.1")
addSbtPlugin("com.lightbend.sbt" % "sbt-java-formatter" % "0.7.0")
// docs
addSbtPlugin("com.lightbend.akka" % "sbt-paradox-akka" % "0.45")
addSbtPlugin("com.lightbend.paradox" % "sbt-paradox-dependencies" % "0.2.2")
addSbtPlugin("com.lightbend.sbt" % "sbt-publish-rsync" % "0.2")
addSbtPlugin("com.github.sbt" % "sbt-unidoc" % "0.5.0")
addSbtPlugin("com.thoughtworks.sbt-api-mappings" % "sbt-api-mappings" % "3.0.2")
addSbtPlugin("com.typesafe.sbt" % "sbt-site" % "1.4.1")
// Akka gRPC -- sync with version in Dependencies.scala:19
addSbtPlugin("com.lightbend.akka.grpc" % "sbt-akka-grpc" % "2.2.0-M1")
// templating
addSbtPlugin("io.spray" % "sbt-boilerplate" % "0.6.1")
