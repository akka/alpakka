/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.reference
import akka.actor.{ActorSystem, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.stream.scaladsl.Flow
import akka.util.ByteString
import com.typesafe.config.Config

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
final class Resource private (val settings: ResourceSettings) {
  // a resource that is to be used when creating Akka Stream operators.
  val connection = Flow[ByteString].map(_.reverse)

  /**
   * Resource cleanup logic
   */
  def cleanup() = {}
}

object Resource {
  def apply(settings: ResourceSettings) = new Resource(settings)

  def create(settings: ResourceSettings) = Resource(settings)
}

/**
 * Settings required for the Resource should be extracted to a separate clas.
 */
final class ResourceSettings private (val msg: String) {
  override def toString: String =
    s"ResourceSettings(msg=$msg)"
}

/**
 * Factories for the settings object should take parameters as well as a `Config`
 * instance for reading values from HOCON.
 */
object ResourceSettings {
  val ConfigPath = "alpakka.reference"

  def apply(msg: String): ResourceSettings = new ResourceSettings(msg)

  /**
   * Java Api
   */
  def create(msg: String): ResourceSettings = ResourceSettings(msg)

  /**
   * Resolves settings from a given Config object, which should have all of the required
   * values at the top level.
   */
  def apply(config: Config): ResourceSettings = {
    val msg = config.getString("msg")
    ResourceSettings(msg)
  }

  /**
   * Java Api
   *
   * Resolves settings from a given Config object, which should have all of the required
   * values at the top level.
   */
  def create(config: Config): ResourceSettings =
    ResourceSettings(config)

  /**
   * Resolves settings from the `ActorSystem`s settings.
   */
  def apply()(implicit sys: ActorSystem): ResourceSettings =
    ResourceSettings(sys.settings.config.getConfig(ConfigPath))

  /**
   * Java Api
   *
   * Resolves settings from the `ActorSystem`s settings.
   */
  def create(sys: ActorSystem): ResourceSettings =
    ResourceSettings()(sys)
}

/**
 * In order to minimise the user facing API, the resource lifetime can be managed by an
 * Akka Extension. In that case Akka Extension will make sure that
 * there is only one instance of the resource instantiated per Actor System.
 */
final class ResourceExt private (sys: ExtendedActorSystem) extends Extension {
  implicit val resource = Resource(ResourceSettings()(sys))

  sys.registerOnTermination(resource.cleanup())
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
