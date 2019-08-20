import sbt._
import sbt.Keys._
import sbt.plugins.JvmPlugin
import org.scalafmt.sbt.ScalafmtPlugin.autoImport._
import de.heikoseeberger.sbtheader._
import de.heikoseeberger.sbtheader.HeaderPlugin.autoImport._
import com.lightbend.paradox.projectinfo.ParadoxProjectInfoPluginKeys._
import Whitesource.whitesourceGroup

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
    apiURL := Some(url(s"https://doc.akka.io/api/alpakka/${version.value}")),
    scmInfo := Some(ScmInfo(url("https://github.com/akka/alpakka"), "git@github.com:akka/alpakka.git")),
    developers += Developer("contributors",
                            "Contributors",
                            "https://gitter.im/akka/dev",
                            url("https://github.com/akka/alpakka/graphs/contributors")),
    licenses := Seq(("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))),
    description := "Alpakka is a Reactive Enterprise Integration library for Java and Scala, based on Reactive Streams and Akka.",
    fatalWarnings := true
  )

  override lazy val projectSettings = Dependencies.Common ++ Seq(
      projectInfoVersion := (if (isSnapshot.value) "snapshot" else version.value),
      whitesourceGroup := Whitesource.Group.Community,
      crossVersion := CrossVersion.binary,
      crossScalaVersions := Dependencies.ScalaVersions,
      scalaVersion := Dependencies.Scala212,
      scalacOptions ++= Seq(
          "-encoding",
          "UTF-8",
          "-feature",
          "-unchecked",
          "-deprecation",
          "-Xlint",
          "-Ywarn-dead-code",
          "-target:jvm-1.8"
        ),
      scalacOptions ++= (scalaVersion.value match {
          case Dependencies.Scala213 => Seq.empty[String]
          case _ => Seq("-Xfuture", "-Yno-adapted-args")
        }),
      scalacOptions ++= (scalaVersion.value match {
          case Dependencies.Scala212 if insideCI.value && fatalWarnings.value && !Dependencies.Nightly =>
            Seq("-Xfatal-warnings")
          case _ => Seq.empty
        }),
      Compile / doc / scalacOptions := scalacOptions.value ++ Seq(
          "-doc-title",
          "Alpakka",
          "-doc-version",
          version.value,
          "-sourcepath",
          (baseDirectory in ThisBuild).value.toString,
          "-doc-source-url", {
            val branch = if (isSnapshot.value) "master" else s"v${version.value}"
            s"https://github.com/akka/alpakka/tree/${branch}€{FILE_PATH}.scala#L1"
          },
          "-skip-packages",
          "akka.pattern:" + // for some reason Scaladoc creates this
          "org.mongodb.scala:" + // this one is a mystery as well
          // excluding generated grpc classes, except the model ones (com.google.pubsub)
          "com.google.api:com.google.cloud:com.google.iam:com.google.logging:" +
          "com.google.longrunning:com.google.protobuf:com.google.rpc:com.google.type"
        ),
      Compile / doc / scalacOptions -= "-Xfatal-warnings",
      compile / javacOptions ++= Seq(
          "-Xlint:unchecked"
        ),
      compile / javacOptions ++= (scalaVersion.value match {
          case Dependencies.Scala212 if insideCI.value && fatalWarnings.value => Seq("-Werror")
          case _ => Seq.empty
        }),
      autoAPIMappings := true,
      apiURL := Some(url(s"https://doc.akka.io/api/alpakka/${version.value}/akka/stream/alpakka/index.html")),
      // show full stack traces and test case durations
      testOptions in Test += Tests.Argument("-oDF"),
      // -a Show stack traces and exception class name for AssertionErrors.
      // -v Log "test run started" / "test started" / "test run finished" events on log level "info" instead of "debug".
      // -q Suppress stdout for successful tests.
      testOptions in Test += Tests.Argument(TestFrameworks.JUnit, "-a", "-v", "-q"),
      // By default scalatest futures time out in 150 ms, dilate that to 600ms.
      // This should not impact the total test time as we don't expect to hit this
      // timeout.
      testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-F", "4"),
      scalafmtOnCompile := true,
      headerLicense := Some(HeaderLicense.Custom("Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>"))
    )
}
