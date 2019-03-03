/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb
import akka.actor.{ActorSystem, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.alpakka.dynamodb.impl.DynamoClientImpl

/**
 * Holds an instance of `DynamoClientImpl`. This is usually created and managed by an extension,
 * but could be manually managed by a user as well.
 */
final class DynamoClient private (settings: DynamoSettings)(implicit system: ActorSystem,
                                                            val materializer: Materializer) {
  final val underlying = new DynamoClientImpl(settings, AwsOp.errorResponseHandler)
}

object DynamoClient {

  /**
   * Create [[DynamoClient]] from [[DynamoSettings]].
   */
  def apply(settings: DynamoSettings)(implicit system: ActorSystem, materializer: Materializer): DynamoClient =
    new DynamoClient(settings)

  /**
   * Java API
   *
   * Create [[DynamoClient]] from [[DynamoSettings]].
   */
  def create(settings: DynamoSettings, system: ActorSystem, materializer: Materializer): DynamoClient =
    DynamoClient(settings)(system, materializer)
}

/**
 * Manages one [[DynamoClient]] per `ActorSystem`.
 */
final class DynamoClientExt private (sys: ExtendedActorSystem) extends Extension {
  private[this] val systemMaterializer = ActorMaterializer()(sys)
  implicit val dynamoClient = DynamoClient(DynamoSettings(sys))(sys, systemMaterializer)
}

object DynamoClientExt extends ExtensionId[DynamoClientExt] with ExtensionIdProvider {
  override def lookup = DynamoClientExt
  override def createExtension(system: ExtendedActorSystem) = new DynamoClientExt(system)
}
