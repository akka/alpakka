/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.googlepubsub.scaladsl

import java.security.PrivateKey

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.alpakka.googlepubsub._
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.{Done, NotUsed}

import scala.concurrent.{ExecutionContext, Future}

object GooglePubSub extends GooglePubSub {
  private[googlepubsub] override val httpApi = HttpApi
  private[googlepubsub] def getSession(clientEmail: String, privateKey: PrivateKey): Session =
    new Session(clientEmail, privateKey)
}

protected[googlepubsub] trait GooglePubSub {
  private[googlepubsub] def httpApi: HttpApi
  private[googlepubsub] def getSession(clientEmail: String, privateKey: PrivateKey): Session

  private implicit def matToEx(implicit mat: Materializer): ExecutionContext = mat.executionContext

  /**
   * Creates a flow to that publish messages to a topic and emits the message ids
   */
  def publish(projectId: String,
              apiKey: String,
              clientEmail: String,
              privateKey: PrivateKey,
              topic: String,
              parallelism: Int = 1)(implicit actorSystem: ActorSystem,
                                    materializer: Materializer): Flow[PublishRequest, Seq[String], NotUsed] = {
    val session = getSession(clientEmail, privateKey)

    Flow[PublishRequest].mapAsync(parallelism) { request =>
      session.getToken().flatMap { accessToken =>
        httpApi.publish(projectId, topic, accessToken, apiKey, request)
      }
    }
  }

  def subscribe(projectId: String, apiKey: String, clientEmail: String, privateKey: PrivateKey, subscription: String)(
      implicit actorSystem: ActorSystem,
      materializer: Materializer): Source[ReceivedMessage, NotUsed] = {
    val session = getSession(clientEmail, privateKey)
    Source.fromGraph(new SubscriptionSourceStage(projectId = projectId, apiKey = apiKey, session = session,
        subscription = subscription, httpApi = httpApi))
  }

  def acknowledge(projectId: String,
                  apiKey: String,
                  clientEmail: String,
                  privateKey: PrivateKey,
                  subscription: String,
                  parallelism: Int = 1)(implicit actorSystem: ActorSystem,
                                        materializer: Materializer): Sink[AcknowledgeRequest, Future[Done]] = {
    val session = getSession(clientEmail, privateKey)

    Flow[AcknowledgeRequest]
      .mapAsync(parallelism) { ackReq =>
        session.getToken().flatMap { accessToken =>
          httpApi.acknowledge(project = projectId, subscription = subscription, accessToken = accessToken,
            apiKey = apiKey, request = ackReq)
        }
      }
      .toMat(Sink.ignore)(Keep.right)
  }
}
