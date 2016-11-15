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
import com.amazonaws.services.sqs.model.QueueDoesNotExistException
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ AsyncWordSpec, BeforeAndAfterAll, Matchers }

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Random

class SqsSourceSpec extends AsyncWordSpec with BeforeAndAfterAll with ScalaFutures with Matchers {

  //#init-mat
  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()
  //#init-mat

  //#init-client
  val credentials = new BasicAWSCredentials("x", "x")
  implicit val sqsClient: AmazonSQSAsyncClient = new AmazonSQSAsyncClient(credentials)
    .withEndpoint("http://localhost:9324")
  //#init-client

  override protected def afterAll(): Unit = {
    Await.ready(system.terminate(), 5.seconds)
  }

  def randomQueueUrl(): String = sqsClient.createQueue(s"queue-${Random.nextInt}").getQueueUrl

  "SqsSource" should {

    "stream a single batch from the queue" in {

      val queue = randomQueueUrl()
      sqsClient.sendMessage(queue, "alpakka")

      SqsSource(queue)
        .take(1)
        .runWith(Sink.seq)
        .map(_.map(_.getBody) should contain("alpakka"))

    }

    "stream multiple batches from the queue" in {

      val queue = randomQueueUrl()

      val input = 1 to 100 map { i => s"alpakka-$i" }

      input foreach { m => sqsClient.sendMessage(queue, m) }

      //#run
      SqsSource(queue)
        .take(100)
        .runWith(Sink.seq)
        .map(_ should have size 100)
      //#run

    }

    "continue streaming if receives an empty response" in {

      val queue = randomQueueUrl()

      val f = SqsSource(queue, SqsSourceSettings(0.seconds, 100))
        .take(1)
        .runWith(Sink.seq)

      sqsClient.sendMessage(queue, s"alpakka")

      f.map(_ should have size 1)
    }

    "should finish immediately if the queue does not exist" in {

      val queue = "http://localhost:9324/queue/not-existing"

      val f = SqsSource(queue)
        .runWith(Sink.seq)

      f.failed.map(_ shouldBe a[QueueDoesNotExistException])
    }
  }
}
