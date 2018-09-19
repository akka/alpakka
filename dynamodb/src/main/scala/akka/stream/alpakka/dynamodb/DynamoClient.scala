/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb
import akka.actor.{ActorSystem, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.alpakka.dynamodb.impl.DynamoClientImpl
import akka.stream.alpakka.dynamodb.scaladsl.DynamoImplicits

final class DynamoClient(settings: DynamoSettings)(implicit system: ActorSystem, val materializer: Materializer) {
  final val underlying = new DynamoClientImpl(settings, DynamoImplicits.errorResponseHandler)
}

object DynamoClient {
  def apply(settings: DynamoSettings)(implicit system: ActorSystem, materializer: Materializer): DynamoClient =
    new DynamoClient(settings)

  def create(settings: DynamoSettings, system: ActorSystem, materializer: Materializer): DynamoClient =
    DynamoClient(settings)(system, materializer)
}

final class DynamoClientExt private (sys: ExtendedActorSystem) extends Extension {
  private[this] val systemMaterializer = ActorMaterializer()(sys)
  implicit val dynamoClient = new DynamoClient(DynamoSettings(sys))(sys, systemMaterializer)
}

object DynamoClientExt extends ExtensionId[DynamoClientExt] with ExtensionIdProvider {
  override def lookup = DynamoClientExt
  override def createExtension(system: ExtendedActorSystem) = new DynamoClientExt(system)
}
