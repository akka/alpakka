/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub.grpc.javadsl

import java.time.Duration
import java.util.concurrent.{CompletableFuture, CompletionStage}

import akka.actor.Cancellable
import akka.dispatch.ExecutionContexts
import akka.stream.alpakka.googlecloud.pubsub.grpc.impl.Setup
import akka.stream.{ActorMaterializer, Attributes}
import akka.stream.javadsl.{Flow, Keep, Sink, Source}
import akka.{Done, NotUsed}
import com.google.pubsub.v1._

import scala.compat.java8.FutureConverters._
import scala.concurrent.Future

/**
 * Google Pub/Sub Akka Stream operator factory.
 */
object GooglePubSub {

  /**
   * Create a flow to publish messages to Google Cloud Pub/Sub. The flow emits responses that contain published
   * message ids.
   *
   * @param parallelism controls how many messages can be in-flight at any given time
   */
  def publish(parallelism: Int): Flow[PublishRequest, PublishResponse, NotUsed] =
    Setup
      .createFlow { implicit mat => implicit attr =>
        Flow
          .create[PublishRequest]
          .mapAsyncUnordered(parallelism, javaFunction(publisher().client.publish))
      }
      .mapMaterializedValue(javaFunction(_ => NotUsed))

  /**
   * Create a source that emits messages for a given subscription.
   *
   * The materialized value can be used to cancel the source.
   *
   * @param request the subscription FQRS and ack deadline fields are mandatory for the request
   * @param pollInterval time between StreamingPullRequest messages are being sent
   */
  def subscribe(request: StreamingPullRequest,
                pollInterval: Duration): Source[ReceivedMessage, CompletableFuture[Cancellable]] =
    Setup
      .createSource { implicit mat => implicit attr =>
        val cancellable = new CompletableFuture[Cancellable]()

        val subsequentRequest = request.toBuilder
          .setSubscription("")
          .setStreamAckDeadlineSeconds(0)
          .build()

        subscriber().client
          .streamingPull(
            Source
              .single(request)
              .concat(
                Source
                  .tick(pollInterval, pollInterval, subsequentRequest)
                  .mapMaterializedValue(javaFunction(cancellable.complete))
              )
          )
          .mapConcat(javaFunction(_.getReceivedMessagesList))
          .mapMaterializedValue(javaFunction(_ => cancellable))
      }
      .mapMaterializedValue(javaFunction(flattenFutureCs))
      .mapMaterializedValue(javaFunction(_.toCompletableFuture))

  /**
   * Create a sink that accepts consumed message acknowledgements.
   *
   * The materialized value completes on stream completion.
   *
   * @param parallelism controls how many acknowledgements can be in-flight at any given time
   */
  def acknowledge(parallelism: Int): Sink[AcknowledgeRequest, CompletionStage[Done]] =
    Setup
      .createSink { implicit mat => implicit attr =>
        Flow
          .create[AcknowledgeRequest]
          .mapAsyncUnordered(parallelism, javaFunction(subscriber().client.acknowledge))
          .toMat(Sink.ignore(), Keep.right[NotUsed, CompletionStage[Done]])
      }
      .mapMaterializedValue(javaFunction(flattenFutureCs))

  /**
   * Helper for creating akka.japi.function.Function instances from Scala
   * functions as Scala 2.11 does not know about SAMs.
   */
  private def javaFunction[A, B](f: A => B): akka.japi.function.Function[A, B] =
    new akka.japi.function.Function[A, B]() {
      override def apply(a: A): B = f(a)
    }

  private def flattenFutureCs[T](f: Future[CompletionStage[T]]): CompletionStage[T] =
    f.map(_.toScala)(ExecutionContexts.sameThreadExecutionContext)
      .flatMap(identity)(ExecutionContexts.sameThreadExecutionContext)
      .toJava

  private def publisher()(implicit mat: ActorMaterializer, attr: Attributes) =
    attr
      .get[PubSubAttributes.Publisher]
      .map(_.publisher)
      .getOrElse(GrpcPublisherExt()(mat.system).publisher)

  private def subscriber()(implicit mat: ActorMaterializer, attr: Attributes) =
    attr
      .get[PubSubAttributes.Subscriber]
      .map(_.subscriber)
      .getOrElse(GrpcSubscriberExt()(mat.system).subscriber)
}
