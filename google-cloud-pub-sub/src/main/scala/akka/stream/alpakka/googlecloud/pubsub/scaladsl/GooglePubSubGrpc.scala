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
      retryOnFailure: Boolean = false,
      maxConsecutiveFailures: Int = 1,
      parallelism: Int = 1
  )(implicit materializer: Materializer): GooglePubSubClient =
    new GooglePubSubGrpc(project, subscription, pubSubConfig, retryOnFailure, maxConsecutiveFailures, parallelism)
}

private[pubsub] class GooglePubSubGrpc(
    project: String,
    subscription: String,
    pubSubConfig: PubSubConfig,
    retryOnFailure: Boolean,
    maxConsecutiveFailures: Int,
    parallelism: Int
)(implicit materializer: Materializer)
    extends GooglePubSubClient {

  def publish: Flow[v1.PublishRequest, v1.PublishResponse, NotUsed] =
    Flow[v1.PublishRequest]
      .mapAsyncUnordered(parallelism)(request => apiFactory.get.publish(request))

  def subscribe(implicit actorSystem: ActorSystem): Source[v1.ReceivedMessage, NotUsed] =
    Source.fromGraph(new GooglePubSubSourceGrpc(parallelism, retryOnFailure, maxConsecutiveFailures, apiFactory))

  def acknowledge: Sink[AcknowledgeRequest, Future[Done]] =
    Flow[AcknowledgeRequest]
      .mapAsyncUnordered(parallelism)(ackReq => apiFactory.get.ackBatch(ackReq.ackIds))
      .toMat(Sink.ignore)(Keep.right)

  private val apiFactory: GrpcApiFactory = new GrpcApiFactory {
    private var api = create()

    override def get: GrpcApi =
      if (api.isHealthy) {
        api
      } else {
        api = create()
        api
      }

    private def create(): GrpcApi = new GrpcApi(project, subscription, pubSubConfig)
  }
}

trait GooglePubSubClient {
  def publish: Flow[v1.PublishRequest, v1.PublishResponse, NotUsed]
  def subscribe(implicit actorSystem: ActorSystem): Source[v1.ReceivedMessage, NotUsed]
  def acknowledge: Sink[AcknowledgeRequest, Future[Done]]
}
