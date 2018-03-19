/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.stomp.scaladsl

import akka.stream.alpakka.stomp.client._
import akka.Done
import akka.stream.scaladsl.{Keep, Sink, Source}
import io.vertx.ext.stomp.Frame

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import io.vertx.core.buffer.{Buffer => VertxBuffer}
import akka.stream.testkit.scaladsl.TestSink

class StompClientSourceSpec extends StompClientSpec {

  "A Stomp Client SourceStage" should {
    "receive a message published in a topic of a stomp server" in {

      import akka.stream.alpakka.stomp.client.Server._

      // #connector-settings
      val host = "localhost"
      val port = 61667
      val destination = "/topic/topic2"
      val settings = ConnectorSettings(DetailsConnectionProvider(host, port, None), Some(destination))
      // #connector-settings

      // create stomp server
      val server = stompServerWithTopicAndQueue(port)

      // #stomp-client-source
      val source: Source[SendingFrame, Future[Done]] = StompClientSource(settings)
      // #stomp-client-source

      // #materialization
      val sink = Sink.head[SendingFrame]
      val (futConnected: Future[Done], futHead: Future[SendingFrame]) = source.toMat(sink)(Keep.both).run()
      // #materialization

      // to make a predictable test, wait until graph connects to stomp server
      Await.ready(futConnected, patience)

      // create another connection, to send to the stomp topic we have registered in SourceStage
      val futstompClient = settings.connectionProvider.get
      val msg = "Hola source"
      val stomp = Await.result(futstompClient, patience)

      stomp.send(
        new Frame().setCommand(Frame.Command.SEND).setDestination(destination).setBody(VertxBuffer.buffer(msg))
      )

      futHead.futureValue.body.map(_.toChar).mkString("") shouldBe msg

      closeAwaitStompServer(server)
    }

    "receive sequentially the messages received in a stomp server when ack them" in {

      import Server._
      val port = 61613
      val server = stompServerWithTopicAndQueue(port)
      val topic = "/topic/mytopic"
      val connectionProvider = DetailsConnectionProvider("localhost", port)
      val settings =
        ConnectorSettings(connectionProvider = connectionProvider, withAck = true, destination = Some(topic))

      // testing source
      val sink = TestSink.probe[String]
      val source: Source[String, Future[Done]] =
        StompClientSource(settings).map(_.body.map(_.toChar).mkString(""))

      val (futConnected, sub) = source.toMat(sink)(Keep.both).run()

      // to make a predictable test, wait until graph connects to stomp server
      Await.ready(futConnected, patience)

      // create another connection, to send to the stomp topic we have registered in SourceStage
      val futstompClient = settings.connectionProvider.get
      val stomp = Await.result(futstompClient, patience)

      def sendToStomp(msg: String) =
        stomp.send(new Frame().setCommand(Frame.Command.SEND).setDestination(topic).setBody(VertxBuffer.buffer(msg)))

      sub.request(1)
      sendToStomp("1")
      sub.expectNext("1")
      sendToStomp("2")
      sub.requestNext("2")
      sendToStomp("3")
      sub.requestNext("3")

      sub.request(1)
      sendToStomp("4")
      sub.expectNext("4")
      sendToStomp("5")
      sendToStomp("6")
      sub.request(4)
      sendToStomp("7")
      sendToStomp("8")
      sendToStomp("9")
      sendToStomp("10")
      sendToStomp("11")
      sub.expectNext("5", "6", "7", "8")
      sub.expectNoMessage(500.millisecond)
      sub.requestNext("9")
      sub.requestNext("10")
      sub.requestNext("11")
      sub.request(10)
      sub.expectNoMessage(500.milliseconds)
      sendToStomp("12")
      sub.expectNext("12")

      closeAwaitStompServer(server)
    }
  }
}
