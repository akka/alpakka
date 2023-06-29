import sbt._
import sbt.Keys._
import sbt.plugins.JvmPlugin
import org.scalafmt.sbt.ScalafmtPlugin.autoImport._
import de.heikoseeberger.sbtheader._
import de.heikoseeberger.sbtheader.HeaderPlugin.autoImport._
import com.lightbend.paradox.projectinfo.ParadoxProjectInfoPluginKeys._
import com.lightbend.sbt.JavaFormatterPlugin.autoImport.javafmtOnCompile
import com.typesafe.tools.mima.plugin.MimaKeys._
import xerial.sbt.Sonatype.autoImport.sonatypeProfileName

object Common extends AutoPlugin {

  object autoImport {
    val fatalWarnings = settingKey[Boolean]("Warnings stop compilation with an error")
  }
  import autoImport._

  override def trigger = allRequirements

  override def requires = JvmPlugin && HeaderPlugin

  override def globalSettings = Seq(
    organization := "com.lightbend.akka",
    organizationName := "Lightbend Inc.",
    organizationHomepage := Some(url("https://www.lightbend.com/")),
    homepage := Some(url("https://doc.akka.io/docs/alpakka/current")),
    scmInfo := Some(ScmInfo(url("https://github.com/akka/alpakka"), "git@github.com:akka/alpakka.git")),
    developers += Developer("contributors",
                            "Contributors",
                            "https://gitter.im/akka/dev",
                            url("https://github.com/akka/alpakka/graphs/contributors")),
    releaseNotesURL := (
        if ((ThisBuild / isSnapshot).value) None
        else Some(url(s"https://github.com/akka/alpakka/releases/tag/v${version.value}"))
      ),
    licenses := {
      val tagOrBranch =
        if (version.value.endsWith("SNAPSHOT")) "main"
        else "v" + version.value
      Seq(("BUSL-1.1", url(s"https://raw.githubusercontent.com/akka/alpakka/${tagOrBranch}/LICENSE")))
    },
    description := "Alpakka is a Reactive Enterprise Integration library for Java and Scala, based on Reactive Streams and Akka.",
    fatalWarnings := true,
    mimaReportSignatureProblems := true,
    // Ignore unused keys which affect documentation
    excludeLintKeys ++= Set(scmInfo, projectInfoVersion, autoAPIMappings)
  )

  override lazy val projectSettings = Dependencies.Common ++ Seq(
      projectInfoVersion := (if (isSnapshot.value) "snapshot" else version.value),
      crossVersion := CrossVersion.binary,
      crossScalaVersions := Dependencies.Scala2Versions,
      scalaVersion := Dependencies.Scala213,
      scalacOptions ++= Seq(
          "-encoding",
          "UTF-8",
          "-feature",
          "-unchecked",
          "-deprecation",
          "-Xlint",
          "-Ywarn-dead-code",
          "-release",
          "8"
        ),
      scalacOptions ++= (scalaVersion.value match {
          case Dependencies.Scala213
              if insideCI.value && fatalWarnings.value && !Dependencies.CronBuild && scalaVersion.value != Dependencies.Scala3 =>
            Seq("-Werror")
          case _ => Seq.empty[String]
        }),
      Compile / doc / scalacOptions := scalacOptions.value ++ Seq(
          "-doc-title",
          "Alpakka",
          "-doc-version",
          version.value,
          "-sourcepath",
          (ThisBuild / baseDirectory).value.toString
        ) ++ {
          // excluding generated grpc classes, except the model ones (com.google.pubsub)
          val skip = "akka.pattern:" + // for some reason Scaladoc creates this
            "org.mongodb.scala:" + // this one is a mystery as well
            // excluding generated grpc classes, except the model ones (com.google.pubsub)
            "com.google.api:com.google.cloud:com.google.iam:com.google.logging:" +
            "com.google.longrunning:com.google.protobuf:com.google.rpc:com.google.type"
          if (scalaBinaryVersion.value.startsWith("3")) {
            Seq(s"-skip-packages:$skip") // different usage in scala3
          } else {
            Seq("-skip-packages", skip)
          }
        },
      Compile / doc / scalacOptions ++=
        Seq(
          "-doc-source-url", {
            val branch = if (isSnapshot.value) "main" else s"v${version.value}"
            s"https://github.com/akka/alpakka/tree/${branch}€{FILE_PATH_EXT}#L€{FILE_LINE}"
          },
          "-doc-canonical-base-url",
          "https://doc.akka.io/api/alpakka/current/"
        ),
      Compile / doc / scalacOptions -= "-Werror",
      compile / javacOptions ++= Seq(
          "-Xlint:cast",
          "-Xlint:deprecation",
          "-Xlint:dep-ann",
          "-Xlint:empty",
          "-Xlint:fallthrough",
          "-Xlint:finally",
          "-Xlint:overloads",
          "-Xlint:overrides",
          "-Xlint:rawtypes",
          // JDK 11 "-Xlint:removal",
          "-Xlint:static",
          "-Xlint:try",
          "-Xlint:unchecked",
          "-Xlint:varargs"
        ),
      compile / javacOptions ++= (scalaVersion.value match {
          case Dependencies.Scala213 if insideCI.value && fatalWarnings.value && !Dependencies.CronBuild =>
            Seq("-Werror")
          case _ => Seq.empty[String]
        }),
      autoAPIMappings := true,
      apiURL := Some(url(s"https://doc.akka.io/api/alpakka/${version.value}/akka/stream/alpakka/index.html")),
      // show full stack traces and test case durations
      Test / testOptions += Tests.Argument("-oDF"),
      // -a Show stack traces and exception class name for AssertionErrors.
      // -v Log "test run started" / "test started" / "test run finished" events on log level "info" instead of "debug".
      // -q Suppress stdout for successful tests.
      Test / testOptions += Tests.Argument(TestFrameworks.JUnit, "-a", "-v", "-q"),
      // By default scalatest futures time out in 150 ms, dilate that to 600ms.
      // This should not impact the total test time as we don't expect to hit this
      // timeout.
      Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-F", "4"),
      scalafmtOnCompile := false,
      javafmtOnCompile := false,
      headerLicense := Some(
          HeaderLicense.Custom("Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>")
        ),
      sonatypeProfileName := "com.lightbend"
    )
}
