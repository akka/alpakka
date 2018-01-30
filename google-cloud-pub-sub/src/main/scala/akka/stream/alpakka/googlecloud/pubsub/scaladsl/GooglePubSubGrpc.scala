/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub.scaladsl

import java.security.PrivateKey

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.alpakka.googlecloud.pubsub._
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}

import com.google.pubsub.v1

import scala.concurrent.Future

object GooglePubSubGrpc {
  def apply(projectI: String, subscriptionI: String, parallelismI: Int, pubSubConfig: PubSubConfig)(
      implicit materializer: Materializer
  ): GooglePubSubGrpc =
    new GooglePubSubGrpc {
      override private[pubsub] def api = new GrpcApi(projectI, subscriptionI, pubSubConfig)

      override private[pubsub] val project = projectI
      override private[pubsub] val subscription = subscriptionI
      override private[pubsub] val parallelism = parallelismI
    }

  @akka.annotation.InternalApi
  private[pubsub] def getSession(clientEmail: String, privateKey: PrivateKey): Session =
    new Session(clientEmail, privateKey)
}

protected[pubsub] trait GooglePubSubGrpc {
  private[pubsub] def api: GrpcApi

  private[pubsub] val project: String
  private[pubsub] val subscription: String
  private[pubsub] val parallelism: Int

  def subscribe(implicit actorSystem: ActorSystem): Source[v1.ReceivedMessage, NotUsed] =
    Source.fromGraph(new GooglePubSubSourceGrpc(parallelism, api))

  def acknowledge: Sink[AcknowledgeRequest, Future[Done]] =
    Flow[AcknowledgeRequest]
      .mapAsyncUnordered(parallelism)(ackReq => api.ackBatch(ackReq.ackIds))
      .toMat(Sink.ignore)(Keep.right)
}
