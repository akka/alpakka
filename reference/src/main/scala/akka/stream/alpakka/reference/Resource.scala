/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.reference
import akka.actor.{ActorSystem, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.stream.scaladsl.Flow
import akka.util.ByteString

final class Resource {
  // a resource that is to be kept once instance per ActorSystem
  val connection = Flow[ByteString]
}

final class ResourceExt private (sys: ExtendedActorSystem) extends Extension {
  implicit val resource = new Resource()
}

object ResourceExt extends ExtensionId[ResourceExt] with ExtensionIdProvider {
  override def lookup = ResourceExt
  override def createExtension(system: ExtendedActorSystem) = new ResourceExt(system)

  /**
   * Access to extension.
   */
  def apply()(implicit system: ActorSystem): ResourceExt = super.apply(system)

  /**
   * Java API
   *
   * Access to extension.
   */
  override def get(system: ActorSystem): ResourceExt = super.get(system)
}
