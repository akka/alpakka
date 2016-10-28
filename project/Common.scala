import sbt._, Keys._

import de.heikoseeberger.sbtheader._
import de.heikoseeberger.sbtheader.HeaderKey._
import com.typesafe.sbt.SbtScalariform
import com.typesafe.sbt.SbtScalariform.ScalariformKeys

object Common extends AutoPlugin {

  val FileHeader = (HeaderPattern.cStyleBlockComment,
    """|/*
       | * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
       | */
       |""".stripMargin)

  override def trigger = allRequirements
  override def requires = plugins.JvmPlugin && HeaderPlugin

  override lazy val projectSettings = SbtScalariform.scalariformSettings ++
    Dependencies.Common ++ Seq(
    organization := "com.lightbend.akka",
    organizationName := "Lightbend Inc.",
    homepage := Some(url("https://github.com/akka/alpakka")),
    scmInfo := Some(ScmInfo(url("https://github.com/akka/alpakka"), "git@github.com:akka/alpakka.git")),
    developers += Developer("contributors", "Contributors", "https://gitter.im/akka/dev", url("https://github.com/akka/alpakka/graphs/contributors")),

    licenses := Seq(("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))),

    scalaVersion := crossScalaVersions.value.head,
    crossScalaVersions := Dependencies.ScalaVersions,
    crossVersion := CrossVersion.binary,

    scalacOptions ++= Seq(
      "-encoding", "UTF-8",
      "-feature",
      "-unchecked",
      "-deprecation",
      //"-Xfatal-warnings",
      "-Xlint",
      "-Yno-adapted-args",
      "-Ywarn-dead-code",
      "-Xfuture"
    ),

    javacOptions ++= Seq(
      "-Xlint:unchecked"
    ),

    autoAPIMappings := true,
    apiURL := Some(url(s"http://doc.akka.io/alpakka/api/${version.value}")),

    // show full stack traces and test case durations
    testOptions in Test += Tests.Argument("-oDF"),

    // -v Log "test run started" / "test started" / "test run finished" events on log level "info" instead of "debug".
    // -a Show stack traces and exception class name for AssertionErrors.
    testOptions += Tests.Argument(TestFrameworks.JUnit, "-v", "-a"),

    headers := headers.value ++ Map(
      "scala" -> FileHeader,
      "java" -> FileHeader
    ),

    ScalariformKeys.preferences in Compile  := formattingPreferences,
    ScalariformKeys.preferences in Test     := formattingPreferences
  )

  def formattingPreferences = {
    import scalariform.formatter.preferences._
    FormattingPreferences()
      .setPreference(RewriteArrowSymbols, false)
      .setPreference(AlignParameters, true)
      .setPreference(AlignSingleLineCaseStatements, true)
      .setPreference(SpacesAroundMultiImports, true)
  }

}
