/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sqs.scaladsl

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.sqs.AmazonSQSAsyncClient
import org.scalatest.{BeforeAndAfterAll, Suite, Tag}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Random

trait DefaultTestContext extends BeforeAndAfterAll { this: Suite =>

  object Integration extends Tag("akka.stream.alpakka.sqs.scaladsl.Integration")

  //#init-mat
  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()
  //#init-mat

  //#init-client
  val credentials = new BasicAWSCredentials("x", "x")
  implicit val sqsClient: AmazonSQSAsyncClient =
    new AmazonSQSAsyncClient(credentials).withEndpoint[AmazonSQSAsyncClient]("http://localhost:9324")
  //#init-client

  def randomQueueUrl(): String = sqsClient.createQueue(s"queue-${Random.nextInt}").getQueueUrl

  override protected def afterAll(): Unit =
    Await.ready(system.terminate(), 5.seconds)
}
