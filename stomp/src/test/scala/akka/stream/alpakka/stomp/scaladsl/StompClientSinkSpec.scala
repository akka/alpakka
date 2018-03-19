/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.stomp.scaladsl

import akka.Done
import akka.stream.OverflowStrategy
import akka.stream.alpakka.stomp.client.StompClientSpec
import io.vertx.ext.stomp.{Frame => VertxFrame}

import scala.collection.mutable.ArrayBuffer
import akka.stream.alpakka.stomp.client._
import akka.stream.scaladsl.{Keep, Sink, Source}

import scala.concurrent.Future

class StompClientSinkSpec extends StompClientSpec {

  "A Stomp Client SinkStage" should {
    "deliver messages to a stomp server and complete when downstream completes" in {

      // #connector-settings
      val host = "localhost"
      val port = 61613
      val topic = "AnyTopic"
      val settings =
        ConnectorSettings(connectionProvider = DetailsConnectionProvider(host, port), destination = Some(topic))
      // #connector-settings

      // test helper: creates a stomp server locally, which will buffer incoming messages for inspection
      import Server._
      val receivedFrameOnServer: ArrayBuffer[VertxFrame] = ArrayBuffer()
      val server = getStompServer(Some(accumulateHandler(f => receivedFrameOnServer += f)), port)

      // #stomp-client-sink
      val sinkToStomp: Sink[SendingFrame, Future[Done]] = StompClientSink(settings)
      // #stomp-client-sink

      // #stomp-client-sink-materialization
      val input = Vector("one", "two")
      val source = Source(input).map(SendingFrame.from)
      val sinkDone = source.runWith(sinkToStomp)
      // #stomp-client-sink-materialization

      sinkDone.futureValue shouldBe Done

      receivedFrameOnServer
        .result()
        .map(_.getBodyAsString) should contain theSameElementsInOrderAs input

      closeAwaitStompServer(server)
    }

    "fail when no destination is set" in {
      // creating a stomp server
      import Server._
      val port = 61667
      val server = getStompServer(None, port)

      val settings = ConnectorSettings(connectionProvider = DetailsConnectionProvider("localhost", port))

      // functionality to test
      val sinkToStomp = StompClientSink(settings)
      val queueSource = Source.queue[SendingFrame](100, OverflowStrategy.backpressure)
      val (queue, sinkDone) = queueSource.toMat(sinkToStomp)(Keep.both).run()

      queue.offer(SendingFrame(Map(), "msg".toCharArray.toVector.map(_.toByte)))

      sinkDone.failed.futureValue shouldBe an[StompProtocolError]

      closeAwaitStompServer(server)
    }

    "settings topic should set frame destination if not already present" in {
      // creating a stomp server
      import Server._
      val receivedFrameOnServer: ArrayBuffer[VertxFrame] = ArrayBuffer()
      val port = 61668
      val server = getStompServer(Some(accumulateHandler(f => receivedFrameOnServer += f)), port)

      // connection settings
      val topic = "AnyTopic"
      val settings =
        ConnectorSettings(connectionProvider = DetailsConnectionProvider("localhost", port), destination = Some(topic))

      // functionality to test
      val sinkToStomp = StompClientSink(settings)
      val queueSource = Source.queue[SendingFrame](100, OverflowStrategy.backpressure)
      val (queue, sinkDone) = queueSource.toMat(sinkToStomp)(Keep.both).run()

      queue.offer(SendingFrame(Map(("destination" -> "another destination")), "msg".toCharArray.toVector.map(_.toByte)))
      queue.offer(SendingFrame(Map(), "msg".toCharArray.toVector.map(_.toByte)))
      queue.complete()
      queue.watchCompletion().futureValue shouldBe Done
      sinkDone.futureValue shouldBe Done

      receivedFrameOnServer.result().map(_.getDestination) should contain theSameElementsInOrderAs Seq(
        "another destination",
        "AnyTopic"
      )

      closeAwaitStompServer(server)
    }

    "handle failure on downstream" in {

      // creating a stomp server
      import Server._
      val receivedFrameOnServer: ArrayBuffer[VertxFrame] = ArrayBuffer()
      val port = 61669
      val server = getStompServer(Some(accumulateHandler(f => receivedFrameOnServer += f)), port)

      // connection settings
      val topic = "AnyTopic"
      val settings =
        ConnectorSettings(connectionProvider = DetailsConnectionProvider("localhost", port), destination = Some(topic))

      // functionality to test
      val sinkToStomp = StompClientSink(settings)
      val queueSource = Source.queue[SendingFrame](100, OverflowStrategy.backpressure)
      val (queue, sinkDone) = queueSource.toMat(sinkToStomp)(Keep.both).run()

      queue.offer(SendingFrame(Map(("destination" -> "another destination")), "msg".toCharArray.toVector.map(_.toByte)))
      queue.offer(SendingFrame(Map(), "msg".toCharArray.toVector.map(_.toByte)))
      queue.fail(new Exception("on purpose"))

      sinkDone.failed.futureValue shouldBe an[Exception]
      sinkDone.failed.futureValue.getMessage shouldBe "on purpose"

      closeAwaitStompServer(server)
    }
  }
}
