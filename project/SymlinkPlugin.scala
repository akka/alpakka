import java.nio.file.Files

import com.typesafe.sbt.site.SitePlugin
import com.typesafe.sbt.site.SitePlugin.autoImport._
import sbt._
import sbt.Keys._

trait SymlinkKeys {
  val symlinkPaths = settingKey[Map[String, String]]("Symbolic links to create: link -> source")
  val symlinkCreate = taskKey[File]("Creates symlinks according to 'symlinkPaths' setting value")
}

object SymlinkPlugin extends AutoPlugin {

  override def requires = SitePlugin
  override def trigger = noTrigger

  object autoImport extends SymlinkKeys {
    val Symlink = config("symlink")
  }
  import autoImport._

  override def projectSettings = symlinkSettings(Symlink)

  def symlinkSettings(config: Configuration): Seq[Setting[_]] =
    inConfig(config)(
      Seq(
        target := (makeSite / target).value,
        symlinkPaths := Map.empty,
        symlinkCreate := createSymlinks(symlinkPaths.value, target.value)
      )
    )

  private def createSymlinks(definitions: Map[String, String], targetDir: File) = {
    definitions.map {
      case (link, source) =>
        val target = targetDir.toPath

        val linkResolved = target.resolve(link)
        linkResolved.getParent.toFile.mkdirs()

        linkResolved.toFile.delete()
        val relative = linkResolved.getParent.relativize(target.resolve(source))
        Files.createSymbolicLink(linkResolved, relative)
    }
    targetDir
  }
}
