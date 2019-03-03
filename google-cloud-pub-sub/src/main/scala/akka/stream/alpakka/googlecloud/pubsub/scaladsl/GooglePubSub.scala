/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub.scaladsl

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.alpakka.googlecloud.pubsub._
import akka.stream.alpakka.googlecloud.pubsub.impl._
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.{Done, NotUsed}

import scala.collection.immutable
import scala.concurrent.Future

object GooglePubSub extends GooglePubSub {
  private[pubsub] override val httpApi = PubSubApi
}

protected[pubsub] trait GooglePubSub {
  private[pubsub] def httpApi: PubSubApi

  /**
   * Creates a flow to that publish messages to a topic and emits the message ids
   */
  def publish(topic: String, config: PubSubConfig, parallelism: Int = 1)(
      implicit actorSystem: ActorSystem,
      materializer: Materializer
  ): Flow[PublishRequest, immutable.Seq[String], NotUsed] = {
    import materializer.executionContext

    if (httpApi.isEmulated) {
      Flow[PublishRequest].mapAsyncUnordered(parallelism) { request =>
        httpApi.publish(config.projectId, topic, maybeAccessToken = None, request)
      }
    } else {
      Flow[PublishRequest].mapAsyncUnordered(parallelism) { request =>
        config.session.getToken().flatMap { accessToken =>
          httpApi.publish(config.projectId, topic, Some(accessToken), request)
        }
      }
    }
  }

  def subscribe(subscription: String, config: PubSubConfig)(
      implicit actorSystem: ActorSystem
  ): Source[ReceivedMessage, NotUsed] =
    Source.fromGraph(
      new GooglePubSubSource(
        projectId = config.projectId,
        session = config.session,
        subscription = subscription,
        returnImmediately = config.pullReturnImmediately,
        maxMessages = config.pullMaxMessagesPerInternalBatch,
        httpApi = httpApi
      )
    )

  def acknowledge(
      subscription: String,
      config: PubSubConfig,
      parallelism: Int = 1
  )(implicit actorSystem: ActorSystem, materializer: Materializer): Sink[AcknowledgeRequest, Future[Done]] = {
    import materializer.executionContext

    (if (httpApi.isEmulated) {
       Flow[AcknowledgeRequest].mapAsyncUnordered(parallelism) { ackReq =>
         httpApi.acknowledge(project = config.projectId,
                             subscription = subscription,
                             maybeAccessToken = None,
                             request = ackReq)
       }
     } else {
       Flow[AcknowledgeRequest]
         .mapAsyncUnordered(parallelism) { ackReq =>
           config.session.getToken().flatMap { accessToken =>
             httpApi.acknowledge(project = config.projectId,
                                 subscription = subscription,
                                 maybeAccessToken = Some(accessToken),
                                 request = ackReq)
           }
         }
     })
      .toMat(Sink.ignore)(Keep.right)
  }
}
