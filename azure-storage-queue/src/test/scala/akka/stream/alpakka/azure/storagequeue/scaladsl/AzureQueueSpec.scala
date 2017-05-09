/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.azure.storagequeue.scaladsl

import org.scalatest._
import com.microsoft.azure.storage._
import com.microsoft.azure.storage.queue._
import scala.concurrent._
import scala.concurrent.duration._
import akka.stream.scaladsl._
import akka.stream._
import akka.testkit._
import akka.actor.ActorSystem
import scala.util.Properties
import akka.stream.alpakka.azure.storagequeue._
import scala.collection.JavaConverters._

// These tests are all live since the Azure Storage Emulator
// does not run on Linux/Docker yet
class AzureQueueSpec extends TestKit(ActorSystem()) with AsyncFlatSpecLike with BeforeAndAfterAll {
  implicit val materializer = ActorMaterializer()

  val timeout = 10.seconds

  val queueOpt: Option[CloudQueue] = {
    Properties.envOrNone("AZURE_CONNECTION_STRING").map { storageConnectionString =>
      val storageAccount = CloudStorageAccount.parse(storageConnectionString)
      val queueClient = storageAccount.createCloudQueueClient
      val r = scala.util.Random
      val queue = queueClient.getQueueReference(s"testqueue${r.nextInt(100000)}")
      queue.createIfNotExists
      queue
    }
  }
  def queue = queueOpt.get

  override def withFixture(test: NoArgAsyncTest) = {
    assume(queueOpt.isDefined, "Queue is not defined. Please set AZURE_CONNECTION_STRING")
    queueOpt.map(_.clear)
    test()
  }
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

  "AzureQueueSource" should "be able to retrieve messages" in {
    val msgs = (1 to 10).map(_ => queueTestMsg)
    msgs.foreach(m => queue.addMessage(m))

    AzureQueueSource(queue)
      .take(10)
      .runWith(Sink.seq)
      .map(
        dequeuedMsgs =>
          assert(msgs.map(_.getMessageContentAsString).toSet == dequeuedMsgs.map(_.getMessageContentAsString).toSet)
      )
  }

  it should "wait for messages queued later" in {
    val msgs = (1 to 10).map(_ => queueTestMsg)

    val futureAssertion = AzureQueueSource(queue, AzureQueueSourceSettings.default.copy(waitTimeSeconds = 1))
      .take(10)
      .runWith(Sink.seq)
      .map(
        dequeuedMsgs =>
          assert(msgs.map(_.getMessageContentAsString).toSet == dequeuedMsgs.map(_.getMessageContentAsString).toSet)
      )
    Thread.sleep(3000)
    msgs.foreach(m => queue.addMessage(m))

    futureAssertion
  }

  it should "observe maxBufferSize, maxBatchSize and not pull too many message in from the CouldQueue into the buffer" in {
    val msgs = (1 to 11).map(_ => queueTestMsg)
    msgs.foreach(m => queue.addMessage(m))

    Await.result(AzureQueueSource(queue, AzureQueueSourceSettings.default.copy(maxBufferSize = 10, maxBatchSize = 10))
                   .take(1)
                   .runWith(Sink.seq),
                 timeout)

    assert(queue.retrieveMessage() != null, "There should be a 12th message on queue")
    assertCannotGetMessageFromQueue
  }

  "AzureQueueSink" should "be able to queue messages" in {
    val msgs = (1 to 10).map(_ => queueTestMsg)
    Await.result(Source(msgs).runWith(AzureQueueSink(queue)), timeout)

    val dequeuedMsgs = queue.retrieveMessages(10).asScala
    assert(msgs.map(_.getMessageContentAsString).toSet == dequeuedMsgs.map(_.getMessageContentAsString).toSet)

  }

  "AzureQueueWithTimeoutsSinks" should "be able to queue messages" in {
    val msgs = (1 to 10).map(_ => (queueTestMsg, 0, 0))
    Await.result(Source(msgs).runWith(AzureQueueWithTimeoutsSink(queue)), timeout)

    val dequeuedMsgs = queue.retrieveMessages(10).asScala
    assert(msgs.map(_._1.getMessageContentAsString).toSet == dequeuedMsgs.map(_.getMessageContentAsString).toSet)
  }

  it should "observe initalTimeout" in {
    val msgs = (1 to 10).map(_ => (queueTestMsg, 0, 120))
    Await.result(Source(msgs).runWith(AzureQueueWithTimeoutsSink(queue)), timeout)

    assertCannotGetMessageFromQueue
  }

  "AzureQueueDeleteSink" should "be able to delete messages" in {
    // When queuing 10 messages
    val msgs = (1 to 10).map(_ => queueTestMsg)
    msgs.foreach(m => queue.addMessage(m))
    // and deleting 10 message
    Await.result(AzureQueueSource(queue).take(10).runWith(AzureQueueDeleteSink(queue)), timeout)

    // then there should be no messages on the queue anymore
    assertCannotGetMessageFromQueue
  }

  it should "fail for messages not on the queue" in {
    val msgs = (1 to 10).map(_ => queueTestMsg)
    assertThrows[java.lang.IllegalArgumentException] {
      Await.result(Source(msgs).runWith(AzureQueueDeleteSink(queue)), timeout)
    }
  }

  "AzureQueueDeleteOrUpdateSink" should "be able to update visibility timeout" in {
    // Queue 10 messages
    val msgs = (1 to 10).map(_ => queueTestMsg)
    msgs.foreach(m => queue.addMessage(m))

    // Update the visibility to much later for 10 messages
    Await.result(
      AzureQueueSource(queue)
        .take(10)
        .map(msg => (msg, UpdateVisibility(120)))
        .runWith(AzureQueueDeleteOrUpdateSink(queue)),
      timeout
    )

    // Now we should not be able to get another one
    assertCannotGetMessageFromQueue
  }

  it should "be able to delete messages" in {
    // Queue 10 messages
    val msgs = (1 to 10).map(_ => queueTestMsg)
    msgs.foreach(m => queue.addMessage(m))

    // Delete 10 messages
    Await.result(
      AzureQueueSource(queue)
        .take(10)
        .map(msg => (msg, Delete))
        .runWith(AzureQueueDeleteOrUpdateSink(queue)),
      timeout
    )

    // Now we should not be able to get another one
    assertCannotGetMessageFromQueue
  }
}
