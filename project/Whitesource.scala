import sbt._
import sbt.Keys._
import sbtwhitesource.WhiteSourcePlugin.autoImport._
import sbtwhitesource._
import scala.sys.process.Process

object Whitesource extends AutoPlugin {

  sealed trait Group {
    def section: String
  }
  object Group {
    case object Community extends Group {
      override val section = ""
    }
    case object Supported extends Group {
      override val section = "supported-"
    }
  }

  val whitesourceGroup: SettingKey[Group] =
    settingKey[Group]("adds to the Whitesource project name to select a group per module")

  override def requires = WhiteSourcePlugin

  override def trigger = allRequirements

  def majorMinor(version: String): Option[String] = """\d+\.\d+""".r.findFirstIn(version)

  override lazy val projectSettings = Seq(
    // do not change the value of whitesourceProduct
    whitesourceProduct := "Lightbend Reactive Platform",
    whitesourceAggregateProjectName := {
      (moduleName in LocalRootProject).value + "-" +
      whitesourceGroup.value.section +
      (
        if (isSnapshot.value)
          if (describe(baseDirectory.value) contains "master") "master"
          else "adhoc"
        else majorMinor((version in LocalRootProject).value).map(_ + "-stable").getOrElse("adhoc")
      )
    },
    whitesourceForceCheckAllDependencies := true,
    whitesourceFailOnError := true
  )

  private def describe(base: File) = Process(Seq("git", "describe", "--all"), base).!!
}
