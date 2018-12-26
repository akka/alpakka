import sbt._, Keys._

object Publish extends AutoPlugin {
  import bintray.BintrayPlugin
  import bintray.BintrayPlugin.autoImport._

  override def trigger = allRequirements
  override def requires = BintrayPlugin

  override def projectSettings = Seq(
    bintrayOrganization := Some("alpakka-backblazeb2"), // TODO: originally "akka"
    bintrayPackage := "alpakka",
    // bintrayRepository := (if (isSnapshot.value) "snapshots" else "maven") // TODO: we commented this
  )
}

object PublishUnidoc extends AutoPlugin {
  import sbtunidoc.BaseUnidocPlugin.autoImport._
  import sbtunidoc.ScalaUnidocPlugin.autoImport.ScalaUnidoc

  override def requires = sbtunidoc.ScalaUnidocPlugin

  def publishOnly(artifactType: String)(config: PublishConfiguration) = {
    val newArts = config.artifacts.filter(_._1.`type` == artifactType)
    config.withArtifacts(newArts)
  }

  override def projectSettings = Seq(
    doc in Compile := (doc in ScalaUnidoc).value,
    target in unidoc in ScalaUnidoc := crossTarget.value / "api",
    publishConfiguration ~= publishOnly(Artifact.DocType),
    publishLocalConfiguration ~= publishOnly(Artifact.DocType)
  )
}
