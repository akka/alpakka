/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.pubsub.scaladsl

import java.security.PrivateKey

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.alpakka.googlecloud.pubsub._
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.{Done, NotUsed}

import scala.collection.immutable
import scala.concurrent.Future

object GooglePubSub extends GooglePubSub {
  private[pubsub] override val httpApi = HttpApi
  @akka.annotation.InternalApi
  private[pubsub] def getSession(clientEmail: String, privateKey: PrivateKey): Session =
    new Session(clientEmail, privateKey)
}

protected[pubsub] trait GooglePubSub {
  private[pubsub] def httpApi: HttpApi

  @akka.annotation.InternalApi
  private[pubsub] def getSession(clientEmail: String, privateKey: PrivateKey): Session

  /**
   * Creates a flow to that publish messages to a topic and emits the message ids
   */
  def publish(projectId: String,
              apiKey: String,
              clientEmail: String,
              privateKey: PrivateKey,
              topic: String,
              parallelism: Int = 1)(
      implicit actorSystem: ActorSystem,
      materializer: Materializer
  ): Flow[PublishRequest, immutable.Seq[String], NotUsed] = {
    import materializer.executionContext

    if (httpApi.isEmulated) {
      Flow[PublishRequest].mapAsyncUnordered(parallelism) { request =>
        httpApi.publish(projectId, topic, maybeAccessToken = None, apiKey, request)
      }
    } else {
      val session = getSession(clientEmail, privateKey)

      Flow[PublishRequest].mapAsyncUnordered(parallelism) { request =>
        session.getToken().flatMap { accessToken =>
          httpApi.publish(projectId, topic, Some(accessToken), apiKey, request)
        }
      }
    }
  }

  def subscribe(projectId: String, apiKey: String, clientEmail: String, privateKey: PrivateKey, subscription: String)(
      implicit actorSystem: ActorSystem
  ): Source[ReceivedMessage, NotUsed] = {
    val session = getSession(clientEmail, privateKey)
    Source.fromGraph(
      new GooglePubSubSource(projectId = projectId,
                             apiKey = apiKey,
                             session = session,
                             subscription = subscription,
                             httpApi = httpApi)
    )
  }

  def acknowledge(
      projectId: String,
      apiKey: String,
      clientEmail: String,
      privateKey: PrivateKey,
      subscription: String,
      parallelism: Int = 1
  )(implicit actorSystem: ActorSystem, materializer: Materializer): Sink[AcknowledgeRequest, Future[Done]] = {
    import materializer.executionContext

    (if (httpApi.isEmulated) {
       Flow[AcknowledgeRequest].mapAsyncUnordered(parallelism) { ackReq =>
         httpApi.acknowledge(project = projectId,
                             subscription = subscription,
                             maybeAccessToken = None,
                             apiKey = apiKey,
                             request = ackReq)
       }
     } else {
       val session = getSession(clientEmail, privateKey)
       Flow[AcknowledgeRequest]
         .mapAsyncUnordered(parallelism) { ackReq =>
           session.getToken().flatMap { accessToken =>
             httpApi.acknowledge(project = projectId,
                                 subscription = subscription,
                                 maybeAccessToken = Some(accessToken),
                                 apiKey = apiKey,
                                 request = ackReq)
           }
         }
     })
      .toMat(Sink.ignore)(Keep.right)
  }
}
