addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.3.7")
addSbtPlugin("com.dwijnand" % "sbt-dynver" % "4.1.1")
addSbtPlugin("com.geirsson" % "sbt-ci-release" % "1.5.6")
// discipline
addSbtPlugin("de.heikoseeberger" % "sbt-header" % "5.6.0")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.0")
addSbtPlugin("com.typesafe" % "sbt-mima-plugin" % "0.7.0")
addSbtPlugin("com.lightbend" % "sbt-whitesource" % "0.1.18")
addSbtPlugin("com.lightbend.sbt" % "sbt-java-formatter" % "0.5.1")
// docs
addSbtPlugin("com.lightbend.akka" % "sbt-paradox-akka" % "0.37")
addSbtPlugin("com.lightbend.paradox" % "sbt-paradox-dependencies" % "0.2.1")
addSbtPlugin("com.lightbend.sbt" % "sbt-publish-rsync" % "0.2")
addSbtPlugin("com.eed3si9n" % "sbt-unidoc" % "0.4.3")
addSbtPlugin("com.thoughtworks.sbt-api-mappings" % "sbt-api-mappings" % "3.0.0")
addSbtPlugin("com.typesafe.sbt" % "sbt-site" % "1.4.1")
// Akka gRPC -- sync with version in Dependencies.scala
addSbtPlugin("com.lightbend.akka.grpc" % "sbt-akka-grpc" % "1.0.0")
// templating
addSbtPlugin("io.spray" % "sbt-boilerplate" % "0.6.1")
