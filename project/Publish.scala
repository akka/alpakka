import sbt._, Keys._

/**
 * For projects that are not to be published.
 */
object NoPublish extends AutoPlugin {
  override def requires = plugins.JvmPlugin

  override def projectSettings = Seq(
    publishArtifact := false,
    publish := {},
    publishLocal := {}
  )
}

object Publish extends AutoPlugin {
  import bintray.BintrayPlugin
  import bintray.BintrayPlugin.autoImport._

  override def trigger = allRequirements
  override def requires = BintrayPlugin

  override def projectSettings = Seq(
    bintrayOrganization := Some("akka"),
    bintrayPackage := "alpakka"
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
