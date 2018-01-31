/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub

import akka.stream.Materializer
import com.google.pubsub.v1.PubsubMessage

/** Pub/Sub **/
import com.google.auth.oauth2.GoogleCredentials
import com.google.pubsub.v1.{PullRequest, SubscriberGrpc, PublisherGrpc, AcknowledgeRequest => AR}
import com.google.pubsub.v1

/** gRPC **/
import akka.stream.alpakka.googlecloud.pubsub.GuavaConversions._
import io.grpc.auth.MoreCallCredentials
import io.grpc.{ManagedChannel, ManagedChannelBuilder, _}

import scala.collection.JavaConverters._
import scala.concurrent.Future

@akka.annotation.InternalApi
private class GrpcApi(project: String, subscription: String, config: PubSubConfig)(
    implicit materializer: Materializer
) {
  import materializer.executionContext

  type AckId = String

  private val subscriptionFqrn = s"projects/$project/subscriptions/$subscription"

  private val grpcChannel: ManagedChannel = {
    val builder = ManagedChannelBuilder.forAddress(config.host, config.port)
    if (config.usePlaintext) builder.usePlaintext(true)
    builder.build()
  }

  private val callCredentials: CallCredentials =
    MoreCallCredentials
      .from(
        GoogleCredentials.getApplicationDefault.createScoped(List("https://www.googleapis.com/auth/pubsub").asJava)
      )

  private val subscriberStub =
    SubscriberGrpc
      .newFutureStub(grpcChannel)
      .withCallCredentials(callCredentials)

  private val publisherStub =
    PublisherGrpc
      .newFutureStub(grpcChannel)
      .withCallCredentials(callCredentials)

  def read: Future[v1.PullResponse] = {
    val pullRequest = PullRequest
      .newBuilder()
      .setSubscription(subscriptionFqrn)
      .setMaxMessages(config.maxMessages)
      .setReturnImmediately(config.returnImmediately)

    subscriberStub.pull(pullRequest.build()).asScalaFuture
  }

  def publish(msg: PubsubMessage, topic: String): Future[v1.PublishResponse] = {
    val pr = v1.PublishRequest
      .newBuilder()
      .addMessages(msg)
      .setTopic(topic)
      .build()

    publisherStub.publish(pr).asScalaFuture
  }

  def ackBatch(ackIds: Seq[AckId]): Future[Unit] = {
    val ackRequest = AR
      .newBuilder()
      .setSubscription(subscriptionFqrn)
      .addAllAckIds(ackIds.asJava)

    subscriberStub.acknowledge(ackRequest.build()).asScalaFuture.map(_ => ())
  }
}
