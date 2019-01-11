import com.typesafe.sbt.site.SitePlugin
import sbt._
import scala.sys.process._

trait PublishRsyncKeys {
  val publishRsyncArtifact = taskKey[(File, String)]("File or directory and a path to publish to")
  val publishRsyncHost = settingKey[String]("hostname to publish to")
  val publishRsync = taskKey[Int]("Deploy using rsync")
}

object PublishRsyncPlugin extends AutoPlugin {

  override def requires = SitePlugin
  override def trigger = noTrigger

  object autoImport extends PublishRsyncKeys
  import autoImport._

  override def projectSettings = publishRsyncSettings()

  def publishRsyncSettings(): Seq[Setting[_]] = Seq(
    publishRsync := {
      val (from, to) = publishRsyncArtifact.value
      s"rsync -azP $from/ ${publishRsyncHost.value}:$to" !
    }
  )
}
