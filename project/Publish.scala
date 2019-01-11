import sbt._, Keys._

object Publish extends AutoPlugin {
  import bintray.BintrayPlugin
  import bintray.BintrayPlugin.autoImport._

  override def trigger = allRequirements
  override def requires = BintrayPlugin

  override def projectSettings = Seq(
    bintrayOrganization := Some("akka"),
    bintrayPackage := "alpakka",
    bintrayRepository := (if (isSnapshot.value) "snapshots" else "maven")
  )
}
