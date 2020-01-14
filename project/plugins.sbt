addSbtPlugin("de.heikoseeberger" % "sbt-header" % "5.2.0")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.2.1")
addSbtPlugin("com.dwijnand" % "sbt-dynver" % "4.0.0")
addSbtPlugin("com.lightbend.paradox" % "sbt-paradox-dependencies" % "0.2")
addSbtPlugin("com.lightbend.akka" % "sbt-paradox-akka" % "0.27")
addSbtPlugin("com.lightbend.sbt" % "sbt-publish-rsync" % "0.1")
addSbtPlugin("com.eed3si9n" % "sbt-unidoc" % "0.4.2")
addSbtPlugin("com.thoughtworks.sbt-api-mappings" % "sbt-api-mappings" % "2.1.0")
addSbtPlugin("org.foundweekends" % "sbt-bintray" % "0.5.5")
addSbtPlugin("com.typesafe" % "sbt-mima-plugin" % "0.6.0")
addSbtPlugin("com.lightbend" % "sbt-whitesource" % "0.1.18")
addSbtPlugin("com.typesafe.sbt" % "sbt-site" % "1.4.0")
addSbtPlugin("com.eed3si9n" % "sbt-unidoc" % "0.4.2")
addSbtPlugin("com.lightbend.sbt" % "sbt-java-formatter" % "0.4.4")
addSbtPlugin("com.lightbend.akka.grpc" % "sbt-akka-grpc" % "0.7.3")
// #grpc-agent
addSbtPlugin("com.lightbend.sbt" % "sbt-javaagent" % "0.1.5")
// #grpc-agent
addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.3.7")
// depend directly on the patched version see https://github.com/akka/alpakka/issues/1388
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.9.2+10-148ba0ff")
// patched version of sbt-dependency-graph and sbt-site
resolvers += Resolver.bintrayIvyRepo("2m", "sbt-plugins")
