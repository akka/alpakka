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
  import sbtunidoc.Plugin._
  import sbtunidoc.Plugin.UnidocKeys._

  override def requires = plugins.JvmPlugin

  def publishOnly(artifactType: String)(config: PublishConfiguration) = {
    val newArts = config.artifacts.filterKeys(_.`type` == artifactType)
    new PublishConfiguration(config.ivyFile, config.resolverName, newArts, config.checksums, config.logging)
  }

  override def projectSettings = unidocSettings ++ Seq(
    doc in Compile := (doc in ScalaUnidoc).value,
    target in unidoc in ScalaUnidoc := crossTarget.value / "api",
    publishConfiguration ~= publishOnly(Artifact.DocType),
    publishLocalConfiguration ~= publishOnly(Artifact.DocType)
  )
}
