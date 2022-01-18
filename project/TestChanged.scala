package akka.stream.alpakka

import scala.collection.immutable
import scala.sys.process._

import sbt._
import sbt.Keys._

object TestChanged extends AutoPlugin {
  override def trigger = allRequirements
  override def requires = plugins.JvmPlugin

  val changedDirectories = taskKey[immutable.Set[String]]("List of touched modules in this PR branch")
  val testChanged = taskKey[Unit]("Test all subprojects with changes compared to master")

  override lazy val buildSettings = Seq(
    changedDirectories := {
      val log = streams.value.log
      val target = "origin/master"

      // TODO could use jgit
      val diffOutput = s"git diff $target --name-only".!!.split("\n")
      val changedDirectories =
        diffOutput
          .map(l => l.trim)
          .map(l => l.takeWhile(_ != '/'))
          .map(new File(_))
          .map(file => if (file.isDirectory) file.toString else "")
          .toSet

      log.info("Detected changes in directories: " + changedDirectories.mkString("[", ", ", "]"))
      changedDirectories
    }
  )

  override lazy val projectSettings = Seq(
    testChanged := Def.taskDyn {
        val skip = Def.setting { task(()) }
        if (shouldBuild(name.value, changedDirectories.value)) Test / test
        else skip
      }.value
  )

  implicit class RegexHelper(val sc: StringContext) extends AnyVal {
    def re: scala.util.matching.Regex = sc.parts.mkString.r
  }

  private def shouldBuild(projectName: String, changedDirectories: Set[String]) = projectName match {
    case "alpakka" => false
    case re"akka-stream-alpakka-(.+)$subproject" =>
      changedDirectories.contains(subproject) || changedDirectories.contains("") || changedDirectories.contains(
        "project"
      )
  }
}
