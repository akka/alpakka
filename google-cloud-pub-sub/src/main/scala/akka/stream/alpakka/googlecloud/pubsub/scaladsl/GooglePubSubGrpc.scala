/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub.scaladsl

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.alpakka.googlecloud.pubsub._
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}

import com.google.pubsub.v1

import scala.concurrent.Future

object GooglePubSubGrpc {
  def apply(
      project: String,
      subscription: String,
      pubSubConfig: PubSubConfig,
      parallelism: Int = 1
  )(implicit materializer: Materializer): GooglePubSubClient =
    new GooglePubSubGrpc(new GrpcApi(project, subscription, pubSubConfig), parallelism)
}

private[pubsub] class GooglePubSubGrpc(
    api: GrpcApi,
    parallelism: Int
) extends GooglePubSubClient {
  def publish: Flow[v1.PublishRequest, v1.PublishResponse, NotUsed] =
    Flow[v1.PublishRequest]
      .mapAsyncUnordered(parallelism)(request => api.publish(request))

  def subscribe(implicit actorSystem: ActorSystem): Source[v1.ReceivedMessage, NotUsed] =
    Source.fromGraph(new GooglePubSubSourceGrpc(parallelism, api))

  def acknowledge: Sink[AcknowledgeRequest, Future[Done]] =
    Flow[AcknowledgeRequest]
      .mapAsyncUnordered(parallelism)(ackReq => api.ackBatch(ackReq.ackIds))
      .toMat(Sink.ignore)(Keep.right)
}

trait GooglePubSubClient {
  def publish: Flow[v1.PublishRequest, v1.PublishResponse, NotUsed]
  def subscribe(implicit actorSystem: ActorSystem): Source[v1.ReceivedMessage, NotUsed]
  def acknowledge: Sink[AcknowledgeRequest, Future[Done]]
}
