package akka

import sbt._
import sbt.Keys._

import Tests._
import de.heikoseeberger.sbtheader._
import de.heikoseeberger.sbtheader.HeaderKey._
import com.typesafe.sbt.SbtScalariform
import com.typesafe.sbt.SbtScalariform.ScalariformKeys

object Common extends AutoPlugin {

  val AkkaVersion = "2.4.9"

  override def trigger = allRequirements
  override def requires = plugins.JvmPlugin && HeaderPlugin

  override lazy val projectSettings = SbtScalariform.scalariformSettings ++ Seq(
    organization := "com.typesafe.akka",
    organizationName := "Lightbend Inc.",

    licenses := Seq(("Apache License, Version 2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))),

    scalaVersion := "2.11.8",
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
      "-Ywarn-numeric-widen",
      "-Xfuture"
    ),

    // show full stack traces and test case durations
    testOptions in Test += Tests.Argument("-oDF"),

    // -v Log "test run started" / "test started" / "test run finished" events on log level "info" instead of "debug".
    // -a Show stack traces and exception class name for AssertionErrors.
    testOptions += Tests.Argument(TestFrameworks.JUnit, "-v", "-a"),

    libraryDependencies ++= Seq(
      "com.typesafe.akka"      %% "akka-stream"                         % AkkaVersion,
      "com.typesafe.akka"      %% "akka-stream-testkit"                 % AkkaVersion   % "test",
      "org.scalatest"          %% "scalatest"                           % "2.2.6"       % "test" // ApacheV2
    ),

    headers := headers.value ++ Map(
      "scala" -> (
        HeaderPattern.cStyleBlockComment,
        """|/*
           | * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
           | */
           |""".stripMargin
      )
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
