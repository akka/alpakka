/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.sns.scaladsl

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.sns.model.PublishRequest
import com.amazonaws.services.sns.{AmazonSNSAsync, AmazonSNSAsyncClientBuilder}

import scala.concurrent.Future

object Examples {

  //#init-client
  val credentials = new BasicAWSCredentials("x", "x")
  implicit val snsClient: AmazonSNSAsync =
    AmazonSNSAsyncClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials)).build()
  //#init-client

  //#init-system
  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: ActorMaterializer = ActorMaterializer()
  //#init-system

  //#use-sink
  val stringSink: Future[Done] =
    Source
      .single("message")
      .runWith(SnsPublisher.sink("topic-arn"))

  val publishRequestSink: Future[Done] =
    Source
      .single(new PublishRequest().withMessage("message"))
      .runWith(SnsPublisher.publishSink("topic-arn"))
  //#use-sink

  //#use-flow
  val stringFLow: Future[Done] =
    Source
      .single("message")
      .via(SnsPublisher.flow("topic-arn"))
      .runWith(Sink.ignore)

  val publishRequestFlow: Future[Done] =
    Source
      .single(new PublishRequest().withMessage("message"))
      .via(SnsPublisher.publishFlow("topic-arn"))
      .runWith(Sink.ignore)
  //#use-flow

}
