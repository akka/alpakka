import sbt._
import sbt.Keys._
import sbt.plugins.JvmPlugin
import com.lucidchart.sbt.scalafmt.ScalafmtCorePlugin.autoImport._
import de.heikoseeberger.sbtheader._
import de.heikoseeberger.sbtheader.HeaderPlugin.autoImport._
import sbtunidoc.BaseUnidocPlugin.autoImport._

object Common extends AutoPlugin {

  override def trigger = allRequirements

  override def requires = JvmPlugin && HeaderPlugin

  override lazy val projectSettings =
  Dependencies.Common ++ Seq(
    organization := "com.lightbend.akka",
    organizationName := "Lightbend Inc.",
    homepage := Some(url("https://github.com/akka/alpakka")),
    scmInfo := Some(ScmInfo(url("https://github.com/akka/alpakka"), "git@github.com:akka/alpakka.git")),
    developers += Developer("contributors",
                            "Contributors",
                            "https://gitter.im/akka/dev",
                            url("https://github.com/akka/alpakka/graphs/contributors")),
    licenses := Seq(("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))),
    crossVersion := CrossVersion.binary,
    scalacOptions ++= Seq(
      "-encoding",
      "UTF-8",
      "-feature",
      "-unchecked",
      "-deprecation",
      //"-Xfatal-warnings",
      "-Xlint",
      "-Yno-adapted-args",
      "-Ywarn-dead-code",
      "-Xfuture"
    ),
    javacOptions in compile ++= Seq(
      "-Xlint:unchecked"
    ),
    autoAPIMappings := true,
    apiURL := Some(url(s"http://developer.lightbend.com/docs/api/alpakka/${version.value}")),
    // show full stack traces and test case durations
    testOptions in Test += Tests.Argument("-oDF"),
    // -a Show stack traces and exception class name for AssertionErrors.
    testOptions in Test += Tests.Argument(TestFrameworks.JUnit, "-a"),
    // By default scalatest futures time out in 150 ms, dilate that to 600ms.
    // This should not impact the total test time as we don't expect to hit this
    // timeout.
    testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-F", "4"),
    scalafmtOnCompile := true,
    headerLicense := Some(HeaderLicense.Custom("Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>"))
  )
}
