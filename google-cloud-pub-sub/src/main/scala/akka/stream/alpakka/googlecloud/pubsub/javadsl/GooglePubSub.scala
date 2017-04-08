/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.googlecloud.pubsub.javadsl

import java.security.PrivateKey
import java.util.concurrent.CompletionStage

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.alpakka.googlecloud.pubsub.{AcknowledgeRequest, PublishRequest, ReceivedMessage}
import akka.stream.alpakka.googlecloud.pubsub.scaladsl.{GooglePubSub => GPubSub}
import akka.stream.javadsl.{Flow, Sink, Source}

import scala.compat.java8.FutureConverters._
import scala.collection.JavaConverters._

object GooglePubSub {

  def publish(projectId: String,
              apiKey: String,
              clientEmail: String,
              privateKey: PrivateKey,
              topic: String,
              parallelism: Int,
              actorSystem: ActorSystem,
              materializer: Materializer): Flow[PublishRequest, java.util.List[String], NotUsed] =
    GPubSub
      .publish(projectId = projectId,
               apiKey = apiKey,
               clientEmail = clientEmail,
               privateKey = privateKey,
               topic = topic,
               parallelism = parallelism)(actorSystem, materializer)
      .map(_.asJava)
      .asJava

  def subscribe(projectId: String,
                apiKey: String,
                clientEmail: String,
                privateKey: PrivateKey,
                subscription: String,
                actorSystem: ActorSystem,
                materializer: Materializer): Source[ReceivedMessage, NotUsed] =
    GPubSub
      .subscribe(projectId = projectId,
                 apiKey = apiKey,
                 clientEmail = clientEmail,
                 privateKey = privateKey,
                 subscription = subscription)(actorSystem, materializer)
      .asJava

  def acknowledge(projectId: String,
                  apiKey: String,
                  clientEmail: String,
                  privateKey: PrivateKey,
                  subscription: String,
                  parallelism: Int,
                  actorSystem: ActorSystem,
                  materializer: Materializer): Sink[AcknowledgeRequest, CompletionStage[Done]] =
    GPubSub
      .acknowledge(projectId = projectId,
                   apiKey = apiKey,
                   clientEmail = clientEmail,
                   privateKey = privateKey,
                   subscription = subscription,
                   parallelism = parallelism)(actorSystem, materializer)
      .mapMaterializedValue(_.toJava)
      .asJava
}
