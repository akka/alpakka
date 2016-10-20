package akka

import sbt._
import sbt.Keys._

import com.lightbend.paradox.sbt.ParadoxPlugin
import com.lightbend.paradox.sbt.ParadoxPlugin.autoImport._

/**
 * This autoplugin adds a command that can be used to override the URI
 * that Paradox uses to link to local API docs. Useful when writing
 * documentation and testing the output in the browser.
 */
object LocalApiDocs extends AutoPlugin {

  override def trigger = allRequirements
  override def requires = ParadoxPlugin

  override lazy val projectSettings = Seq(
    commands += Command.command("localApiDocs") { state =>
      val extracted = Project.extract(state)
      import extracted._
      append(Seq(paradoxProperties += "scaladoc.akka.stream.contrib.base_url" -> s"../../../../target/unidoc"), state)
    }
  )

}
