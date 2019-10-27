/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp.scaladsl

import java.util.concurrent.TimeoutException

import akka.Done
import akka.stream.alpakka.amqp._
import akka.stream.scaladsl.{Flow, FlowWithContext, Keep, Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.util.ByteString
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.ConfirmCallback
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.mockito.{ArgumentCaptor, Mockito}
import org.scalatest.BeforeAndAfterEach

import scala.concurrent.Future
import scala.concurrent.duration._

/**
 * Some tests need a local running AMQP server on the default port with no password.
 */
class AmqpFlowSpec extends AmqpSpec with AmqpMocking with BeforeAndAfterEach {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(10.seconds)

  override def afterEach(): Unit =
    reset(channelMock)

  def confirmationCallbacks(): (ConfirmCallback, ConfirmCallback) = {
    val confirmCallbackCaptor = ArgumentCaptor.forClass(classOf[ConfirmCallback])
    val rejectCallbackCaptor = ArgumentCaptor.forClass(classOf[ConfirmCallback])

    verify(channelMock, Mockito.timeout(patienceConfig.timeout.toMillis))
      .addConfirmListener(confirmCallbackCaptor.capture, rejectCallbackCaptor.capture)

    (confirmCallbackCaptor.getValue, rejectCallbackCaptor.getValue)
  }

  def amqpWriteSettings(connectionProvider: AmqpConnectionProvider): AmqpWriteSettings = {
    val queueName = "amqp-flow-spec" + System.currentTimeMillis()
    val queueDeclaration = QueueDeclaration(queueName)
    AmqpWriteSettings(connectionProvider)
      .withRoutingKey(queueName)
      .withDeclaration(queueDeclaration)
  }

  def localAmqpWriteSettings: AmqpWriteSettings =
    amqpWriteSettings(AmqpLocalConnectionProvider)

  def mockAmqpWriteSettings: AmqpWriteSettings =
    amqpWriteSettings(AmqpConnectionFactoryConnectionProvider(connectionFactoryMock))

  "The AMQP simple flow" should {
    "emit confirmation for published messages" in assertAllStagesStopped {
      val localSimpleFlow = AmqpFlow.apply[String](localAmqpWriteSettings)
      shouldEmitConfirmationForPublishedMessages(localSimpleFlow)
    }

    "fail stage on publication error" in assertAllStagesStopped {
      val mockedSimpleFlow = AmqpFlow.apply[String](mockAmqpWriteSettings)
      shouldFailStageOnPublicationError(mockedSimpleFlow)
    }

    "propagate context" in assertAllStagesStopped {
      val localSimpleFlowWithContext = AmqpFlowWithContext.apply[String](localAmqpWriteSettings)
      shouldPropagateContext(localSimpleFlowWithContext)
    }
  }

  "The AMQP confirmation flow" should {

    val mockedBlockingFlow = AmqpFlow.withConfirm[String](mockAmqpWriteSettings, 200.millis)

    "emit confirmation for published messages" in assertAllStagesStopped {
      val localBlockingFlow = AmqpFlow.withConfirm[String](localAmqpWriteSettings, 200.millis)
      shouldEmitConfirmationForPublishedMessages(localBlockingFlow)
    }

    "fail stage on publication error" in assertAllStagesStopped {
      shouldFailStageOnPublicationError(mockedBlockingFlow)
    }

    "propagate context" in assertAllStagesStopped {
      val localSimpleFlowWithContext = AmqpFlowWithContext.apply[String](localAmqpWriteSettings)
      shouldPropagateContext(localSimpleFlowWithContext)
    }

    "emit rejected result on message rejection" in assertAllStagesStopped {
      when(channelMock.waitForConfirms(any[Long]))
        .thenReturn(true)
        .thenReturn(false)

      val input = Vector("one", "two")

      val (completion, probe) =
        Source(input)
          .map(s => WriteMessage(ByteString(s)))
          .viaMat(mockedBlockingFlow)(Keep.right)
          .toMat(TestSink.probe)(Keep.both)
          .run

      val messages = probe.request(input.size).expectNextN(input.size)

      messages should contain inOrder (WriteResult.confirmed, WriteResult.rejected)
      completion.futureValue shouldBe an[Done]
    }

    "emit rejected result on confirmation timeout" in assertAllStagesStopped {
      when(channelMock.waitForConfirms(any[Long]))
        .thenReturn(true)
        .thenThrow(new TimeoutException())

      val input = Vector("one", "two")

      val (completion, probe) =
        Source(input)
          .map(s => WriteMessage(ByteString(s)))
          .viaMat(mockedBlockingFlow)(Keep.right)
          .toMat(TestSink.probe)(Keep.both)
          .run

      val messages = probe.request(input.size).expectNextN(input.size)

      messages should contain inOrder (WriteResult.confirmed, WriteResult.rejected)
      completion.futureValue shouldBe an[Done]
    }
  }

  "The AMQP async confirmation flow" should {
    val mockedAsyncFlow = AmqpFlow.withAsyncConfirm(mockAmqpWriteSettings, 10, 200.millis)

    "emit confirmation for published messages" in assertAllStagesStopped {
      val localAsyncFlow = AmqpFlow.withAsyncConfirm(localAmqpWriteSettings, 10, 200.millis)
      shouldEmitConfirmationForPublishedMessages(localAsyncFlow)
    }

    "fail stage on publication error" in assertAllStagesStopped {
      shouldFailStageOnPublicationError(mockedAsyncFlow)
    }

    "propagate context" in assertAllStagesStopped {
      val localAsyncFlowWithContext =
        AmqpFlowWithContext.withAsyncConfirm[String](localAmqpWriteSettings, 10, 200.millis)
      shouldPropagateContext(localAsyncFlowWithContext)
    }

    "emit rejected result on message rejection" in assertAllStagesStopped {
      shouldEmitRejectedResultOnMessageRejection(mockedAsyncFlow)
    }

    "emit rejected result on confirmation timeout" in assertAllStagesStopped {
      shouldEmitRejectedResultOnConfirmationTimeout(mockedAsyncFlow)
    }

    "emit multiple results on batch confirmation" in assertAllStagesStopped {
      shouldEmitMultipleResultsOnBatchConfirmation(mockedAsyncFlow)
    }

    "not pull when message buffer is full" in assertAllStagesStopped {
      shouldNotPullWhenMessageBufferIsFull(mockedAsyncFlow)
    }

    "process all buffered messages on upstream finish" in assertAllStagesStopped {
      shouldProcessAllBufferedMessagesOnUpstreamFinish(mockedAsyncFlow)
    }

    "preserve upstream order in emitted messages" in assertAllStagesStopped {
      val mockedAsyncFlowWithContext =
        AmqpFlowWithContext.withAsyncConfirm[String](mockAmqpWriteSettings, 10, 200.millis)

      val deliveryTags = 1L to 7L
      when(channelMock.getNextPublishSeqNo).thenReturn(deliveryTags.head, deliveryTags.tail: _*)

      val input = Vector("one", "two", "three", "four", "five", "six", "seven")

      val (completion, probe) =
        Source(input)
          .asSourceWithContext(identity)
          .map(s => WriteMessage(ByteString(s)))
          .viaMat(mockedAsyncFlowWithContext)(Keep.right)
          .asSource
          .toMat(TestSink.probe)(Keep.both)
          .run

      probe.request(input.size)

      verify(channelMock, Mockito.timeout(patienceConfig.timeout.toMillis).times(input.length))
        .basicPublish(any[String], any[String], any[Boolean], any[Boolean], any[BasicProperties], any[Array[Byte]])

      val (confirmCallback, rejectCallback) = confirmationCallbacks()

      confirmCallback.handle(deliveryTags(1), false)
      confirmCallback.handle(deliveryTags(2), true)
      rejectCallback.handle(deliveryTags(6), false)
      rejectCallback.handle(deliveryTags(5), true)

      val messages = probe.expectNextN(input.size)

      val expectedResult = Seq(
        (WriteResult.confirmed, input(0)),
        (WriteResult.confirmed, input(1)),
        (WriteResult.confirmed, input(2)),
        (WriteResult.rejected, input(3)),
        (WriteResult.rejected, input(4)),
        (WriteResult.rejected, input(5)),
        (WriteResult.rejected, input(6))
      )

      messages should contain theSameElementsInOrderAs expectedResult
      completion.futureValue shouldBe an[Done]
    }
  }

  "AMQP unordered async confirmation flow" should {
    val mockedAsyncUnorderedFlow =
      AmqpFlow.withAsyncUnorderedConfirm[String](mockAmqpWriteSettings, 10, 200.millis)

    "emit confirmation for published messages" in assertAllStagesStopped {
      val localAsyncUnorderedFlow =
        AmqpFlow.withAsyncUnorderedConfirm[String](localAmqpWriteSettings, 10, 200.millis)
      shouldEmitConfirmationForPublishedMessages(localAsyncUnorderedFlow)
    }

    "fail stage on publication error" in assertAllStagesStopped {
      shouldFailStageOnPublicationError(mockedAsyncUnorderedFlow)
    }

    "propagate context" in assertAllStagesStopped {
      val localAsyncUnorderedFlowWithContext =
        AmqpFlowWithContext.withAsyncUnorderedConfirm[String](localAmqpWriteSettings, 10, 200.millis)
      shouldPropagateContext(localAsyncUnorderedFlowWithContext)
    }

    "emit rejected result on message rejection" in assertAllStagesStopped {
      shouldEmitRejectedResultOnMessageRejection(mockedAsyncUnorderedFlow)
    }

    "emit rejected result on confirmation timeout" in assertAllStagesStopped {
      shouldEmitRejectedResultOnConfirmationTimeout(mockedAsyncUnorderedFlow)
    }

    "emit multiple results on batch confirmation" in assertAllStagesStopped {
      shouldEmitMultipleResultsOnBatchConfirmation(mockedAsyncUnorderedFlow)
    }

    "not pull when message buffer is full" in assertAllStagesStopped {
      shouldNotPullWhenMessageBufferIsFull(mockedAsyncUnorderedFlow)
    }

    "process all buffered messages on upstream finish" in assertAllStagesStopped {
      shouldProcessAllBufferedMessagesOnUpstreamFinish(mockedAsyncUnorderedFlow)
    }

    "emit messages in order of received confirmations" in assertAllStagesStopped {
      val mockedAsyncUnorderedFlowWithContext =
        AmqpFlowWithContext.withAsyncUnorderedConfirm[String](mockAmqpWriteSettings, 10, 200.millis)

      val deliveryTags = 1L to 7L
      when(channelMock.getNextPublishSeqNo).thenReturn(deliveryTags.head, deliveryTags.tail: _*)

      val input = Vector("one", "two", "three", "four", "five", "six", "seven")

      val (completion, probe) =
        Source(input)
          .asSourceWithContext(identity)
          .map(s => WriteMessage(ByteString(s)))
          .viaMat(mockedAsyncUnorderedFlowWithContext)(Keep.right)
          .asSource
          .toMat(TestSink.probe)(Keep.both)
          .run

      probe.request(input.size)

      verify(channelMock, Mockito.timeout(patienceConfig.timeout.toMillis).times(input.length))
        .basicPublish(any[String], any[String], any[Boolean], any[Boolean], any[BasicProperties], any[Array[Byte]])

      val (confirmCallback, rejectCallback) = confirmationCallbacks()

      confirmCallback.handle(deliveryTags(1), false)
      confirmCallback.handle(deliveryTags(2), true)
      rejectCallback.handle(deliveryTags(6), false)
      rejectCallback.handle(deliveryTags(5), true)

      val messages = probe.expectNextN(input.size)

      val expectedResult = Seq(
        (WriteResult.confirmed, input(1)),
        (WriteResult.confirmed, input(0)),
        (WriteResult.confirmed, input(2)),
        (WriteResult.rejected, input(6)),
        (WriteResult.rejected, input(3)),
        (WriteResult.rejected, input(4)),
        (WriteResult.rejected, input(5))
      )

      messages should contain theSameElementsInOrderAs expectedResult
      completion.futureValue shouldBe an[Done]
    }
  }

  def shouldEmitConfirmationForPublishedMessages(flow: Flow[WriteMessage, WriteResult, Future[Done]]) = {
    val input = Vector("one", "two", "three", "four", "five")
    val expectedOutput = input.map(_ => WriteResult.confirmed)

    val (completion, probe) =
      Source(input)
        .map(s => WriteMessage(ByteString(s)))
        .viaMat(flow)(Keep.right)
        .toMat(TestSink.probe)(Keep.both)
        .run

    val messages = probe.request(input.size).expectNextN(input.size)

    messages should contain theSameElementsAs expectedOutput
    completion.futureValue shouldBe an[Done]
  }

  def shouldPropagateContext(flow: FlowWithContext[WriteMessage, String, WriteResult, String, Future[Done]]) = {
    val input = Vector("one", "two", "three", "four", "five")
    val expectedOutput = input.map(s => (WriteResult.confirmed, s))

    val (completion, probe) =
      Source(input)
        .asSourceWithContext(identity)
        .map(s => WriteMessage(ByteString(s)))
        .viaMat(flow)(Keep.right)
        .toMat(TestSink.probe)(Keep.both)
        .run

    val messages = probe.request(input.size).expectNextN(input.size)

    messages should contain theSameElementsAs expectedOutput
    completion.futureValue shouldBe an[Done]
  }

  def shouldFailStageOnPublicationError(flow: Flow[WriteMessage, WriteResult, Future[Done]]) = {
    val publicationError = new RuntimeException("foo")

    when(
      channelMock
        .basicPublish(any[String], any[String], any[Boolean], any[Boolean], any[BasicProperties], any[Array[Byte]])
    ).thenThrow(publicationError)

    val completion =
      Source
        .single("one")
        .map(s => WriteMessage(ByteString(s)))
        .via(flow)
        .runWith(Sink.ignore)

    completion.failed.futureValue shouldEqual publicationError
  }

  def shouldEmitRejectedResultOnMessageRejection(flow: Flow[WriteMessage, WriteResult, Future[Done]]) = {

    val deliveryTags = 1L to 2L
    when(channelMock.getNextPublishSeqNo).thenReturn(deliveryTags(0), deliveryTags(1))

    val input = Vector("one", "two")

    val (completion, probe) =
      Source(input)
        .map(s => WriteMessage(ByteString(s)))
        .viaMat(flow)(Keep.right)
        .toMat(TestSink.probe)(Keep.both)
        .run

    probe.request(input.size)

    verify(channelMock, Mockito.timeout(patienceConfig.timeout.toMillis).times(input.length))
      .basicPublish(any[String], any[String], any[Boolean], any[Boolean], any[BasicProperties], any[Array[Byte]])

    val (confirmCallback, rejectCallback) = confirmationCallbacks()

    confirmCallback.handle(deliveryTags(0), false)
    rejectCallback.handle(deliveryTags(1), false)

    val messages = probe.expectNextN(input.size)

    messages should contain theSameElementsAs Seq(WriteResult.confirmed, WriteResult.rejected)
    completion.futureValue shouldBe an[Done]
  }

  def shouldEmitRejectedResultOnConfirmationTimeout(flow: Flow[WriteMessage, WriteResult, Future[Done]]) = {
    when(channelMock.getNextPublishSeqNo).thenReturn(1L, 2L)

    val input = Vector("one", "two")

    val (completion, probe) =
      Source(input)
        .map(s => WriteMessage(ByteString(s)))
        .viaMat(flow)(Keep.right)
        .toMat(TestSink.probe)(Keep.both)
        .run

    val messages = probe.request(input.size).expectNextN(input.size)

    messages should contain theSameElementsAs Seq(WriteResult.rejected, WriteResult.rejected)
    completion.futureValue shouldBe an[Done]
  }

  def shouldEmitMultipleResultsOnBatchConfirmation(flow: Flow[WriteMessage, WriteResult, Future[Done]]) = {
    val deliveryTags = 1L to 5L
    when(channelMock.getNextPublishSeqNo).thenReturn(deliveryTags.head, deliveryTags.tail: _*)

    val input = Vector("one", "two", "three", "four", "five")

    val (completion, probe) =
      Source(input)
        .map(s => WriteMessage(ByteString(s)))
        .viaMat(flow)(Keep.right)
        .toMat(TestSink.probe)(Keep.both)
        .run

    probe.request(input.size)

    verify(channelMock, Mockito.timeout(patienceConfig.timeout.toMillis).times(input.length))
      .basicPublish(any[String], any[String], any[Boolean], any[Boolean], any[BasicProperties], any[Array[Byte]])

    val (confirmCallback, rejectCallback) = confirmationCallbacks()

    confirmCallback.handle(deliveryTags(1), true)
    confirmCallback.handle(deliveryTags(2), false)
    rejectCallback.handle(deliveryTags(4), true)

    val messages = probe.expectNextN(input.size)

    val expectedResult = Seq(
      WriteResult.confirmed,
      WriteResult.confirmed,
      WriteResult.confirmed,
      WriteResult.rejected,
      WriteResult.rejected
    )

    messages should contain theSameElementsAs expectedResult
    completion.futureValue shouldBe an[Done]
  }

  def shouldNotPullWhenMessageBufferIsFull(flow: Flow[WriteMessage, WriteResult, Future[Done]]) = {
    val bufferSize = 10
    val sourceElements = bufferSize + 1

    val deliveryTags = 1L to 10L
    when(channelMock.getNextPublishSeqNo).thenReturn(deliveryTags.head, deliveryTags.tail: _*)

    val probe =
      Source(1 to sourceElements)
        .map(s => WriteMessage(ByteString(s)))
        .viaMat(flow)(Keep.right)
        .toMat(TestSink.probe)(Keep.right)
        .run

    probe.request(sourceElements)

    verify(channelMock, Mockito.timeout(200).times(bufferSize)).getNextPublishSeqNo

    probe.cancel()
  }

  def shouldProcessAllBufferedMessagesOnUpstreamFinish(flow: Flow[WriteMessage, WriteResult, Future[Done]]) = {
    when(channelMock.getNextPublishSeqNo).thenReturn(1L, 2L)

    val input = Vector("one", "two")

    val (sourceProbe, sinkProbe) =
      TestSource
        .probe[String]
        .map(s => WriteMessage(ByteString(s)))
        .viaMat(flow)(Keep.left)
        .toMat(TestSink.probe)(Keep.both)
        .run

    sinkProbe.request(input.size)
    input.foreach(sourceProbe.sendNext)

    sourceProbe.sendComplete()

    val (confirmCallback, _) = confirmationCallbacks()

    verify(channelMock, Mockito.timeout(200).times(input.size)).getNextPublishSeqNo

    confirmCallback.handle(2L, true)

    val messages = sinkProbe.expectNextN(input.size)

    messages should contain theSameElementsAs Seq(WriteResult.confirmed, WriteResult.confirmed)

  }
}
