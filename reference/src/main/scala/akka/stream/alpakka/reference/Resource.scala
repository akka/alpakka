/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.reference
import akka.actor.{ActorSystem, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.stream.scaladsl.Flow
import akka.util.ByteString

/**
 * Some connectors might require an external resource that is used in the
 * Source, Flow and/or Sink factories.
 *
 * For example dynamodb connector needs a DynamoClient to create Sources and Flows.
 * Another example is Google Pub Sub gRPC connector that uses Grpc Publishers and
 * Subscribers to create Sources and Sinks. Another connector, Alpakka Kafka, uses
 * an actor that can be shared across different streams.
 *
 * If your connector uses such a resource and it is possible to reuse that resource
 * across different Akka Stream operator factories, put that resource to a separate
 * class like below.
 */
final class Resource {
  // a resource that is to be used when creating Akka Stream operators.
  val connection = Flow[ByteString]
}

/**
 * In order to minimise the user facing API, the resource lifetime can be managed by an
 * Akka Extension. In that case Akka Extension will make sure that
 * there is only one instance of the resource instantiated per Actor System.
 */
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
