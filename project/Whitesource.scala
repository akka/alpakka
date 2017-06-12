import sbt._
import sbt.Keys._
import sbtwhitesource.WhiteSourcePlugin.autoImport._
import sbtwhitesource._
import com.typesafe.sbt.SbtGit.GitKeys._

object Whitesource extends AutoPlugin {
  override def requires = WhiteSourcePlugin

  override lazy val projectSettings = Seq(
    // do not change the value of whitesourceProduct
    whitesourceProduct := "Lightbend Reactive Platform",
    whitesourceAggregateProjectName in ThisBuild  := {
       "alpakka-" + (
         if (version.value.endsWith("SNAPSHOT"))
           if (gitCurrentBranch.value == "master") "master"
          else "adhoc"
         else majorMinor(version.value).map(_ + "-current").getOrElse("snapshot"))
     },
     whitesourceAggregateProjectToken in ThisBuild := {
       // These are not secrets but integration identifiers:
       whitesourceAggregateProjectName.value match {
         case "alpakka-master" => "91b0af7e-083c-4f14-a5f4-6409550e0e4b"
         case "alpakka-adhoc" => "6e848d2b-461c-45ce-abcb-1c9a05851278"
         case other => throw new Exception(s"Please add project '$other' to whitesource and record the integration token here")
       }
     }
   )

   def majorMinor(version: String): Option[String] ="""\d+\.\d+""".r.findFirstIn(version)
}
