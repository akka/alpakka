/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub.grpc.javadsl

import akka.actor.{ActorSystem, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.annotation.ApiMayChange
import akka.stream.alpakka.googlecloud.pubsub.grpc.PubSubSettings
import akka.stream.alpakka.googlecloud.pubsub.grpc.impl.AkkaGrpcSettings
import akka.stream.{ActorMaterializer, Materializer}
import com.google.pubsub.v1.{SubscriberClient => JavaSubscriberClient}

/**
 * Holds the gRPC java subscriber client instance.
 */
final class GrpcSubscriber private (settings: PubSubSettings, sys: ActorSystem, mat: Materializer) {

  @ApiMayChange
  final val client =
    JavaSubscriberClient.create(AkkaGrpcSettings.fromPubSubSettings(settings)(sys), mat, sys.dispatcher)

  sys.registerOnTermination(client.close())
}

object GrpcSubscriber {
  def create(settings: PubSubSettings, sys: ActorSystem, mat: Materializer): GrpcSubscriber =
    new GrpcSubscriber(settings, sys, mat)

  def create(sys: ActorSystem, mat: Materializer): GrpcSubscriber =
    create(PubSubSettings(sys), sys, mat)
}

/**
 * An extension that manages a single gRPC java subscriber client per actor system.
 */
final class GrpcSubscriberExt private (sys: ExtendedActorSystem) extends Extension {
  private[this] val systemMaterializer = ActorMaterializer()(sys)

  implicit val subscriber = GrpcSubscriber.create(sys, systemMaterializer)
}

object GrpcSubscriberExt extends ExtensionId[GrpcSubscriberExt] with ExtensionIdProvider {
  override def lookup = GrpcSubscriberExt
  override def createExtension(system: ExtendedActorSystem) = new GrpcSubscriberExt(system)

  /**
   * Access to extension.
   */
  def apply()(implicit system: ActorSystem): GrpcSubscriberExt = super.apply(system)

  /**
   * Java API
   *
   * Access to extension.
   */
  override def get(system: ActorSystem): GrpcSubscriberExt = super.get(system)
}
