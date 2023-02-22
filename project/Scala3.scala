import sbt.Keys.crossScalaVersions

object Scala3 {

  val settings = Seq(crossScalaVersions := Dependencies.ScalaVersions)

}
