/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.amqp.scaladsl

import akka.Done
import akka.stream.alpakka.amqp._
import akka.stream.scaladsl.{Flow, FlowWithContext, Keep, Sink, Source}
import akka.stream.testkit.TestSubscriber
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
      .withBufferSize(10)
      .withConfirmationTimeout(200.millis)
  }

  def localAmqpWriteSettings: AmqpWriteSettings =
    amqpWriteSettings(AmqpLocalConnectionProvider)

  def mockAmqpWriteSettings: AmqpWriteSettings =
    amqpWriteSettings(AmqpConnectionFactoryConnectionProvider(connectionFactoryMock))

  "The AMQP simple flow" should {
    "emit confirmation for published messages" in assertAllStagesStopped {
      val localSimpleFlow = AmqpFlow.apply(localAmqpWriteSettings)
      shouldEmitConfirmationForPublishedMessages(localSimpleFlow)
    }

    "fail stage on publication error" in assertAllStagesStopped {
      val mockedSimpleFlow = AmqpFlow.apply(mockAmqpWriteSettings)
      shouldFailStageOnPublicationError(mockedSimpleFlow)
    }

    "propagate context" in assertAllStagesStopped {
      val localSimpleFlowWithContext = AmqpFlowWithContext.apply[String](localAmqpWriteSettings)
      shouldPropagateContext(localSimpleFlowWithContext)
    }
  }

  "The AMQP confirmation flow" should {
    val mockedFlowWithConfirm = AmqpFlow.withConfirm(mockAmqpWriteSettings)

    "emit confirmation for published messages" in assertAllStagesStopped {
      val localFlowWithConfirm = AmqpFlow.withConfirm(localAmqpWriteSettings)
      shouldEmitConfirmationForPublishedMessages(localFlowWithConfirm)
    }

    "fail stage on publication error" in assertAllStagesStopped {
      shouldFailStageOnPublicationError(mockedFlowWithConfirm)
    }

    "fail stage on creating channel error" in assertAllStagesStopped {
      new AmqpMocking {
        def shouldFailStageOnCreatingChannelError(flow: Flow[WriteMessage, WriteResult, Future[Done]]) = {
          val channelError = new RuntimeException("channel error")

          when(
            connectionMock.createChannel()
          ).thenThrow(channelError)

          val completion =
            Source
              .single("one")
              .map(s => WriteMessage(ByteString(s)))
              .via(flow)
              .runWith(Sink.ignore)

          completion.failed.futureValue shouldEqual channelError
        }

        val mockedFlowWithConfirm =
          AmqpFlow.withConfirm(amqpWriteSettings(AmqpConnectionFactoryConnectionProvider(connectionFactoryMock)))
        shouldFailStageOnCreatingChannelError(mockedFlowWithConfirm)
      }
    }

    "propagate context" in assertAllStagesStopped {
      val localFlowWithContextAndConfirm =
        AmqpFlowWithContext.withConfirm[String](localAmqpWriteSettings)
      shouldPropagateContext(localFlowWithContextAndConfirm)
    }

    "emit rejected result on message rejection" in assertAllStagesStopped {
      shouldEmitRejectedResultOnMessageRejection(mockedFlowWithConfirm)
    }

    "emit rejected result on confirmation timeout" in assertAllStagesStopped {
      shouldEmitRejectedResultOnConfirmationTimeout(mockedFlowWithConfirm)
    }

    "emit multiple results on batch confirmation" in assertAllStagesStopped {
      shouldEmitMultipleResultsOnBatchConfirmation(mockedFlowWithConfirm)
    }

    "not pull when message buffer is full" in assertAllStagesStopped {
      shouldNotPullWhenMessageBufferIsFull(mockedFlowWithConfirm)
    }

    "process all buffered messages on upstream finish" in assertAllStagesStopped {
      shouldProcessAllBufferedMessagesOnUpstreamFinish(mockedFlowWithConfirm)
    }

    "preserve upstream order in emitted messages" in assertAllStagesStopped {
      val mockedFlowWithContextAndConfirm =
        AmqpFlowWithContext.withConfirm[String](mockAmqpWriteSettings)

      val deliveryTags = 1L to 7L
      when(channelMock.getNextPublishSeqNo).thenReturn(deliveryTags.head, deliveryTags.tail: _*)

      val input = Vector("one", "two", "three", "four", "five", "six", "seven")

      val (completion, probe) =
        Source(input)
          .asSourceWithContext(identity)
          .map(s => WriteMessage(ByteString(s)))
          .viaMat(mockedFlowWithContextAndConfirm)(Keep.right)
          .asSource
          .toMat(TestSink())(Keep.both)
          .run()

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

  "AMQP unordered confirmation flow" should {
    val mockedFlowWithConfirmUnordered =
      AmqpFlow.withConfirmUnordered(mockAmqpWriteSettings)

    "emit confirmation for published messages" in assertAllStagesStopped {
      val localFlowWithConfirmUnordered =
        AmqpFlow.withConfirmUnordered(localAmqpWriteSettings)
      shouldEmitConfirmationForPublishedMessages(localFlowWithConfirmUnordered)
    }

    "fail stage on publication error" in assertAllStagesStopped {
      shouldFailStageOnPublicationError(mockedFlowWithConfirmUnordered)
    }

    "propagate pass-through" in assertAllStagesStopped {
      val localFlowWithConfirmAndPassThroughUnordered =
        AmqpFlow.withConfirmAndPassThroughUnordered[String](localAmqpWriteSettings)
      shouldPropagatePassThrough(localFlowWithConfirmAndPassThroughUnordered)
    }

    "emit rejected result on message rejection" in assertAllStagesStopped {
      shouldEmitRejectedResultOnMessageRejection(mockedFlowWithConfirmUnordered)
    }

    "emit rejected result on confirmation timeout" in assertAllStagesStopped {
      shouldEmitRejectedResultOnConfirmationTimeout(mockedFlowWithConfirmUnordered)
    }

    "emit multiple results on batch confirmation" in assertAllStagesStopped {
      shouldEmitMultipleResultsOnBatchConfirmation(mockedFlowWithConfirmUnordered)
    }

    "not pull when message buffer is full" in assertAllStagesStopped {
      shouldNotPullWhenMessageBufferIsFull(mockedFlowWithConfirmUnordered)
    }

    "process all buffered messages on upstream finish" in assertAllStagesStopped {
      shouldProcessAllBufferedMessagesOnUpstreamFinish(mockedFlowWithConfirmUnordered)
    }

    "emit messages in order of received confirmations" in assertAllStagesStopped {
      val mockedUnorderedFlowWithPassThrough =
        AmqpFlow.withConfirmAndPassThroughUnordered[String](mockAmqpWriteSettings)

      val deliveryTags = 1L to 7L
      when(channelMock.getNextPublishSeqNo).thenReturn(deliveryTags.head, deliveryTags.tail: _*)

      val input = Vector("one", "two", "three", "four", "five", "six", "seven")

      val (completion, probe) =
        Source(input)
          .asSourceWithContext(identity)
          .map(s => WriteMessage(ByteString(s)))
          .viaMat(mockedUnorderedFlowWithPassThrough)(Keep.right)
          .asSource
          .toMat(TestSink())(Keep.both)
          .run()

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
        .toMat(TestSink())(Keep.both)
        .run()

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
        .toMat(TestSink())(Keep.both)
        .run()

    val messages = probe.request(input.size).expectNextN(input.size)

    messages should contain theSameElementsAs expectedOutput
    completion.futureValue shouldBe an[Done]
  }

  def shouldPropagatePassThrough(flow: Flow[(WriteMessage, String), (WriteResult, String), Future[Done]]) = {
    val input = Vector("one", "two", "three", "four", "five")
    val expectedOutput = input.map(s => (WriteResult.confirmed, s))

    val (completion: Future[Done], probe: TestSubscriber.Probe[(WriteResult, String)]) =
      Source(input)
        .map(s => (WriteMessage(ByteString(s)), s))
        .viaMat(flow)(Keep.right)
        .toMat(TestSink())(Keep.both)
        .run()

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
        .toMat(TestSink())(Keep.both)
        .run()

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
        .toMat(TestSink())(Keep.both)
        .run()

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
        .toMat(TestSink())(Keep.both)
        .run()

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
        .toMat(TestSink())(Keep.right)
        .run()

    probe.request(sourceElements)

    verify(channelMock, Mockito.timeout(200).times(bufferSize)).getNextPublishSeqNo

    probe.cancel()
  }

  def shouldProcessAllBufferedMessagesOnUpstreamFinish(flow: Flow[WriteMessage, WriteResult, Future[Done]]) = {
    when(channelMock.getNextPublishSeqNo).thenReturn(1L, 2L)

    val input = Vector("one", "two")

    val (sourceProbe, sinkProbe) =
      TestSource[String]()
        .map(s => WriteMessage(ByteString(s)))
        .viaMat(flow)(Keep.left)
        .toMat(TestSink())(Keep.both)
        .run()

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
