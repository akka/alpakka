/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.alpakka.azure.storagequeue.DeleteOrUpdateMessage.{Delete, UpdateVisibility}
import akka.stream.alpakka.azure.storagequeue._
import akka.stream.alpakka.azure.storagequeue.scaladsl._
import akka.stream.scaladsl._
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.testkit._
import com.microsoft.azure.storage._
import com.microsoft.azure.storage.queue._
import org.scalatest._

import scala.collection.JavaConverters._
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.Properties
import org.scalatest.flatspec.AsyncFlatSpecLike

// These tests are all live since the Azure Storage Emulator
// does not run on Linux/Docker yet
class AzureQueueSpec extends TestKit(ActorSystem()) with AsyncFlatSpecLike with BeforeAndAfterAll {
  implicit val materializer = ActorMaterializer()

  val timeout = 10.seconds
  val queueName = s"testqueue${scala.util.Random.nextInt(100000)}"
  val azureConnStringOpt = Properties.envOrNone("AZURE_CONNECTION_STRING")

  def queueOpt: Option[CloudQueue] =
    azureConnStringOpt.map { storageConnectionString =>
      val storageAccount = CloudStorageAccount.parse(storageConnectionString)
      val queueClient = storageAccount.createCloudQueueClient
      val queue = queueClient.getQueueReference(queueName)
      queue
    }
  val queueFactory = () => queueOpt.get
  def queue = queueFactory()

  override def withFixture(test: NoArgAsyncTest) = {
    assume(queueOpt.isDefined, "Queue is not defined. Please set AZURE_CONNECTION_STRING")
    queueOpt.map(_.clear)
    test()
  }
  override def beforeAll: Unit =
    queueOpt.map(_.createIfNotExists)
  override def afterAll: Unit = {
    queueOpt.map(_.deleteIfExists)
    TestKit.shutdownActorSystem(system)
    super.afterAll
  }

  private var testMsgCount = 0
  def queueTestMsg: CloudQueueMessage = {
    val message = new CloudQueueMessage(s"Test message no. ${testMsgCount}")
    testMsgCount += 1
    message
  }

  def assertCannotGetMessageFromQueue =
    assert(queue.peekMessage() == null)

  "AzureQueueSource" should "be able to retrieve messages" in assertAllStagesStopped {
    val msgs = (1 to 10).map(_ => queueTestMsg)
    msgs.foreach(m => queue.addMessage(m))

    AzureQueueSource(queueFactory)
      .runWith(Sink.seq)
      .map(dequeuedMsgs =>
        assert(msgs.map(_.getMessageContentAsString).toSet == dequeuedMsgs.map(_.getMessageContentAsString).toSet)
      )
  }

  it should "observe retrieveRetryTimeout and retrieve messages queued later" in assertAllStagesStopped {
    val msgs = (1 to 10).map(_ => queueTestMsg)

    val futureAssertion =
      AzureQueueSource(queueFactory, AzureQueueSourceSettings().withRetrieveRetryTimeout(1.seconds))
        .take(10)
        .runWith(Sink.seq)
        .map(dequeuedMsgs =>
          assert(msgs.map(_.getMessageContentAsString).toSet == dequeuedMsgs.map(_.getMessageContentAsString).toSet)
        )
    Thread.sleep(3000)
    msgs.foreach(m => queue.addMessage(m))

    futureAssertion
  }

  it should "observe batchSize and not pull too many message in from the CouldQueue into the buffer" in assertAllStagesStopped {
    val msgs = (1 to 20).map(_ => queueTestMsg)
    msgs.foreach(m => queue.addMessage(m))

    Await.result(AzureQueueSource(queueFactory, AzureQueueSourceSettings().withBatchSize(2))
                   .take(1)
                   .runWith(Sink.seq),
                 timeout
    )

    assert(queue.retrieveMessage() != null, "There should be a 11th message on queue")
  }

  "AzureQueueSink" should "be able to queue messages" in assertAllStagesStopped {
    val msgs = (1 to 10).map(_ => queueTestMsg)
    Await.result(Source(msgs).runWith(AzureQueueSink(queueFactory)), timeout)

    val dequeuedMsgs = queue.retrieveMessages(10).asScala
    assert(msgs.map(_.getMessageContentAsString).toSet == dequeuedMsgs.map(_.getMessageContentAsString).toSet)

  }

  "AzureQueueWithTimeoutsSinks" should "be able to queue messages" in assertAllStagesStopped {
    val msgs = (1 to 10).map(_ => (queueTestMsg, 0, 0))
    Await.result(Source(msgs).runWith(AzureQueueWithTimeoutsSink(queueFactory)), timeout)

    val dequeuedMsgs = queue.retrieveMessages(10).asScala
    assert(msgs.map(_._1.getMessageContentAsString).toSet == dequeuedMsgs.map(_.getMessageContentAsString).toSet)
  }

  it should "observe initalTimeout" in assertAllStagesStopped {
    val msgs = (1 to 10).map(_ => (queueTestMsg, 0, 120))
    Await.result(Source(msgs).runWith(AzureQueueWithTimeoutsSink(queueFactory)), timeout)

    assertCannotGetMessageFromQueue
  }

  "AzureQueueDeleteSink" should "be able to delete messages" in assertAllStagesStopped {
    // When queuing 10 messages
    val msgs = (1 to 10).map(_ => queueTestMsg)
    msgs.foreach(m => queue.addMessage(m))
    // and deleting 10 message
    Await.result(AzureQueueSource(queueFactory).take(10).runWith(AzureQueueDeleteSink(queueFactory)), timeout)

    // then there should be no messages on the queue anymore
    assertCannotGetMessageFromQueue
  }

  it should "fail for messages not on the queue" in assertAllStagesStopped {
    val msgs = (1 to 10).map(_ => queueTestMsg)
    assertThrows[java.lang.IllegalArgumentException] {
      Await.result(Source(msgs).runWith(AzureQueueDeleteSink(queueFactory)), timeout)
    }
  }

  "AzureQueueDeleteOrUpdateSink" should "be able to update visibility timeout" in assertAllStagesStopped {
    // Queue 10 messages
    val msgs = (1 to 10).map(_ => queueTestMsg)
    msgs.foreach(m => queue.addMessage(m))

    // Update the visibility to much later for 10 messages
    Await.result(
      AzureQueueSource(queueFactory)
        .take(10)
        .map(msg => (msg, UpdateVisibility(120)))
        .runWith(AzureQueueDeleteOrUpdateSink(queueFactory)),
      timeout
    )

    // Now we should not be able to get another one
    assertCannotGetMessageFromQueue
  }

  it should "be able to delete messages" in assertAllStagesStopped {
    // Queue 10 messages
    val msgs = (1 to 10).map(_ => queueTestMsg)
    msgs.foreach(m => queue.addMessage(m))

    // Delete 10 messages
    Await.result(
      AzureQueueSource(queueFactory)
        .take(10)
        .map(msg => (msg, Delete))
        .runWith(AzureQueueDeleteOrUpdateSink(queueFactory)),
      timeout
    )

    // Now we should not be able to get another one
    assertCannotGetMessageFromQueue
  }
}
