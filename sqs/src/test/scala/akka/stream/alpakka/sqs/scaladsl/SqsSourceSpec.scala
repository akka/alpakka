/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.sqs.scaladsl

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.sqs.SqsSourceSettings
import akka.stream.scaladsl.Sink
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.sqs.AmazonSQSAsyncClient
import org.elasticmq.rest.sqs.{SQSRestServer, SQSRestServerBuilder}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Random, Try}

class SqsSourceSpec extends WordSpec with BeforeAndAfterAll with ScalaFutures with Matchers {

  //#init-mat
  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()
  //#init-mat

  //#init-client
  val credentials = new BasicAWSCredentials("x","x")
  implicit val sqsClient: AmazonSQSAsyncClient = new AmazonSQSAsyncClient(credentials)
    .withEndpoint("http://localhost:9324")
  //#init-client

  val server: SQSRestServer = SQSRestServerBuilder
    .withActorSystem(system)
    .withPort(9324)
    .withInterface("localhost")
    .start()

  override protected def afterAll(): Unit = {
    server.stopAndWait()
    Await.ready(system.terminate(), 5.seconds)
  }

  def randomQueueUrl(): String = sqsClient.createQueue(s"queue-${Random.nextInt}").getQueueUrl

  "SqsSource" should {

    "stream a single batch from the queue" in {

      val queue = randomQueueUrl()

      sqsClient.sendMessage(queue, "alpakka")

      val f = SqsSource(queue)
        .takeWithin(100.millis)
        .runWith(Sink.seq)

      f.futureValue.map(_.getBody) should contain("alpakka")
    }

    "stream multiple batches from the queue" in {

      val queue = randomQueueUrl()

      val input = 1 to 100 map { i => s"alpakka-$i" }

      input foreach { m => sqsClient.sendMessage(queue, m) }

      //#run
      val f = SqsSource(queue)
        .takeWithin(100.millis)
        .runWith(Sink.seq)
      //#run

      f.futureValue.map(_.getBody) should have size 100
    }

    //This test has to block for a seconds because it is not possible to go below 1 second with the wait time
    "continue streaming if receives an empty response" in {

      val queue = randomQueueUrl()

      val f = SqsSource(queue, SqsSourceSettings(1.second, 100))
        .takeWithin(1100.millis)
        .runWith(Sink.seq)

      Thread.sleep(1000)

      sqsClient.sendMessage(queue, s"alpakka")

      f.futureValue should have size 1
    }

    "should finish immediately if the queue does not exist" in {

      val queue = "http://localhost:9324/queue/not-existing"

      val f = SqsSource(queue)
        .runWith(Sink.seq)

      Await.ready(f, 1.seconds)
    }
  }
}
