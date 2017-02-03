/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.amqp.scaladsl

import akka.Done
import akka.stream._
import akka.stream.alpakka.amqp._
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, Merge, Sink, Source}
import akka.stream.testkit.TestSubscriber.Probe
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.testkit.{TestPublisher, TestSubscriber}
import akka.util.ByteString

import scala.concurrent.{Await, Promise}
import scala.concurrent.duration._

/**
 * Needs a local running AMQP server on the default port with no password.
 */
class AmqpConnectorsSpec extends AmqpSpec {

  override implicit val patienceConfig = PatienceConfig(10.seconds)

  "The AMQP Connectors" should {

    val connector = new AmqpConnector {}

    val settings = DefaultAmqpConnection

    val connection = connector.newConnection(connector.connectionFactoryFrom(settings), settings)

    val conn = CachedAmqpConnection(connection)

    "publish and consume elements through a simple queue again in the same JVM" in {

      // use a list of host/port pairs where one is normally invalid, but
      // it should still work as expected,
      val connectionSettings =
        AmqpConnectionDetails(List(("invalid", 5673))).withHostsAndPorts(("localhost", 5672))

      //#queue-declaration
      val queueName = "amqp-conn-it-spec-simple-queue-" + System.currentTimeMillis()
      val queueDeclaration = QueueDeclaration(queueName)
      //#queue-declaration

      //#create-sink
      val amqpSink = AmqpSink.simple(
        AmqpSinkSettings(connectionSettings).withRoutingKey(queueName).withDeclarations(queueDeclaration)
      )
      //#create-sink

      //#create-source
      val amqpSource = AmqpSource(
        NamedQueueSourceSettings(connectionSettings, queueName).withDeclarations(queueDeclaration),
        bufferSize = 10
      )
      //#create-source

      //#run-sink
      val input = Vector("one", "two", "three", "four", "five")
      Source(input).map(s => ByteString(s)).runWith(amqpSink)
      //#run-sink

      //#run-source
      val result = amqpSource.map(_.bytes.utf8String).take(input.size).runWith(Sink.seq)
      //#run-source

      result.futureValue shouldEqual input
    }

    "publish via RPC and then consume through a simple queue again in the same JVM" in {

      val queueName = "amqp-conn-it-spec-rpc-queue-" + System.currentTimeMillis()
      val queueDeclaration = QueueDeclaration(queueName)

      //#create-rpc-flow
      val amqpRpcFlow = AmqpRpcFlow.simple(
        AmqpSinkSettings(conn).withRoutingKey(queueName).withDeclarations(queueDeclaration)
      )
      //#create-rpc-flow

      val amqpSource = AmqpSource(
        NamedQueueSourceSettings(conn, queueName),
        bufferSize = 1
      )

      val input = Vector("one", "two", "three", "four", "five")
      //#run-rpc-flow
      val (rpcQueueF, probe) =
        Source(input).map(s => ByteString(s)).viaMat(amqpRpcFlow)(Keep.right).toMat(TestSink.probe)(Keep.both).run
      //#run-rpc-flow
      val rpqCqueue = rpcQueueF.futureValue

      val amqpSink = AmqpSink.replyTo(
        AmqpReplyToSinkSettings(conn)
      )

      amqpSource
        .map(b => OutgoingMessage[ByteString](b.bytes.concat(ByteString("a")), false, false, Some(b.properties)))
        .runWith(amqpSink)

      probe.request(5).expectNextUnorderedN(input.map(s => ByteString(s.concat("a")))).expectComplete()
    }

    "publish via RPC which expects 2 responses per message and then consume through a simple queue again in the same JVM" in {
      val queueName = "amqp-conn-it-spec-rpc-queue-" + System.currentTimeMillis()
      val queueDeclaration = QueueDeclaration(queueName)

      val amqpRpcFlow = AmqpRpcFlow.simple(
        AmqpSinkSettings(conn).withRoutingKey(queueName).withDeclarations(queueDeclaration),
        2
      )

      val amqpSource = AmqpSource(
        NamedQueueSourceSettings(conn, queueName),
        bufferSize = 1
      )

      val input = Vector("one", "two", "three", "four", "five")
      val (rpcQueueF, probe) =
        Source(input).map(s => ByteString(s)).viaMat(amqpRpcFlow)(Keep.right).toMat(TestSink.probe)(Keep.both).run
      val rpqCqueue = rpcQueueF.futureValue

      val amqpSink = AmqpSink.replyTo(
        AmqpReplyToSinkSettings(conn)
      )

      amqpSource
        .mapConcat { b =>
          List(
            OutgoingMessage[ByteString](b.bytes.concat(ByteString("a")), false, false, Some(b.properties)),
            OutgoingMessage[ByteString](b.bytes.concat(ByteString("aa")), false, false, Some(b.properties))
          )
        }
        .runWith(amqpSink)

      probe
        .request(10)
        .expectNextUnorderedN(input.flatMap(s => List(ByteString(s.concat("a")), ByteString(s.concat("aa")))))
        .expectComplete()
    }

    "correctly close a AmqpRpcFlow when stream is closed without passing any elements" in {

      Source
        .empty[ByteString]
        .via(AmqpRpcFlow.simple(AmqpSinkSettings(conn)))
        .runWith(TestSink.probe)
        .ensureSubscription()
        .expectComplete()

    }

    "handle missing reply-to header correctly" in {

      val outgoingMessage = OutgoingMessage[ByteString](ByteString.empty, false, false, None)

      Source
        .single(outgoingMessage)
        .watchTermination()(Keep.right)
        .to(AmqpSink.replyTo(AmqpReplyToSinkSettings(conn)))
        .run()
        .futureValue shouldBe akka.Done

      val caught = intercept[RuntimeException] {
        Source
          .single(outgoingMessage)
          .toMat(AmqpSink.replyTo(AmqpReplyToSinkSettings(conn, true)))(Keep.right)
          .run()
          .futureValue
      }

      caught.getCause.getMessage should equal("Reply-to header was not set")

    }

    "handle manually acked sources" in {
      val queueName = "amqp-conn-it-spec-ack-queue-" + System.currentTimeMillis()
      val queueDeclaration = QueueDeclaration(queueName)

      val amqpSink = AmqpSink.simple(
        AmqpSinkSettings(conn).withRoutingKey(queueName).withDeclarations(queueDeclaration)
      )

      val input = Vector("one", "two", "three", "four", "five")

      Await.result(Source(input).map(s => ByteString(s)).runWith(amqpSink), 1.second)

      val amqpSource: Probe[UnackedIncomingMessage[ByteString]] = AmqpSource
        .withoutAutoAck(
          NamedQueueSourceSettings(conn, queueName),
          bufferSize = 1
        )
        .take(input.size)
        .runWith(TestSink.probe)
        .request(1)

      input.reverse.tail.reverse.foreach { e =>
        val n = amqpSource.expectNext()

        n.message.bytes.utf8String should equal(e)

        amqpSource.request(1)
        amqpSource.expectNoMsg(200.milliseconds)
        n.ack
      }

      val n = amqpSource.expectNext()
      n.message.bytes.utf8String should equal("five")

      amqpSource
        .expectComplete()

    }

    "handle acking sources by passing a flow" in {
      val queueName = "amqp-conn-it-spec-ack2-queue-" + System.currentTimeMillis()
      val queueDeclaration = QueueDeclaration(queueName)

      val amqpSink = AmqpSink.simple(
        AmqpSinkSettings(conn).withRoutingKey(queueName).withDeclarations(queueDeclaration)
      )

      val input = Vector("one", "two", "three", "four", "five")
      Await.result(Source(input).map(s => ByteString(s)).runWith(amqpSink), 1.second)

      val amqpSource: Probe[IncomingMessage[ByteString]] = AmqpSource
        .withoutAutoAck(
          NamedQueueSourceSettings(conn, queueName),
          bufferSize = 1,
          Flow[IncomingMessage[ByteString]].map(_.bytes)
        )
        .take(input.size)
        .runWith(TestSink.probe)
        .request(1)

      input.reverse.tail.reverse.foreach { e =>
        val n = amqpSource.expectNext()

        n.bytes.utf8String should equal(e)

        amqpSource.request(1)
      }

      val n = amqpSource.expectNext()
      n.bytes.utf8String should equal("five")

      amqpSource
        .expectComplete()

    }

    "handle nacking/requeueing messages on substream failure" in {
      val queueName = "amqp-conn-it-spec-ack3-queue-" + System.currentTimeMillis()
      val queueDeclaration = QueueDeclaration(queueName)

      val amqpSink = AmqpSink.simple(
        AmqpSinkSettings(conn).withRoutingKey(queueName).withDeclarations(queueDeclaration)
      )

      val input = Vector("one", "two", "three", "four", "five")
      Await.result(Source(input).map(s => ByteString(s)).runWith(amqpSink), 1.second)

      def amqpSource: Probe[IncomingMessage[ByteString]] =
        AmqpSource
          .withoutAutoAck(
            NamedQueueSourceSettings(conn, queueName),
            bufferSize = 1,
            Flow[IncomingMessage[ByteString]].map { m =>
              m.bytes.utf8String match {
                case "two" if !m.envelope.isRedeliver => throw new Exception("")
                case _ => m.bytes
              }

            }
          )
          .runWith(TestSink.probe)

      val src = amqpSource
        .request(1)

      src.expectNext().bytes.utf8String should equal("one")

      src.request(1)
      src.expectError()

      val src2 = amqpSource
        .request(4)

      val two = src2.expectNext()
      two.envelope.isRedeliver shouldBe true

      val rest = src2.expectNextN(3)

      rest.map(_.bytes.utf8String) should be(Seq("three", "four", "five"))

      src2.request(1)
      src2.expectNoMsg()

    }

    "requeue nacked sources" in {
      val queueName = "amqp-conn-it-spec-nack-queue-" + System.currentTimeMillis()
      val queueDeclaration = QueueDeclaration(queueName)

      val amqpSink = AmqpSink.simple(
        AmqpSinkSettings(conn).withRoutingKey(queueName).withDeclarations(queueDeclaration)
      )

      val input = Vector("one")
      Await.result(Source(input).map(s => ByteString(s)).runWith(amqpSink), 1.second)

      val amqpSource: Probe[UnackedIncomingMessage[ByteString]] = AmqpSource
        .withoutAutoAck(
          NamedQueueSourceSettings(conn, queueName),
          bufferSize = 5
        )
        .runWith(TestSink.probe)
        .request(1)

      val first = amqpSource.expectNext()

      first.message.bytes.utf8String should equal("one")
      first.nack(true)

      amqpSource.request(1)
      val second = amqpSource.expectNext()

      second.message.bytes.utf8String should equal("one")
      second.nack(false)
      amqpSource.request(1)

      amqpSource.expectNoMsg()
    }

    "publish from one source and consume elements with multiple sinks" in {
      val queueName = "amqp-conn-it-spec-work-queues-" + System.currentTimeMillis()
      val queueDeclaration = QueueDeclaration(queueName)
      val amqpSink = AmqpSink.simple(
        AmqpSinkSettings(conn).withRoutingKey(queueName).withDeclarations(queueDeclaration)
      )

      val input = Vector("one", "two", "three", "four", "five")
      Source(input).map(s => ByteString(s)).runWith(amqpSink)

      val mergedSources = Source.fromGraph(GraphDSL.create() { implicit b =>
        import GraphDSL.Implicits._
        val count = 3
        val merge = b.add(Merge[IncomingMessage[ByteString]](count))
        for (n <- 0 until count) {
          val source = b.add(
            AmqpSource(
              NamedQueueSourceSettings(conn, queueName).withDeclarations(queueDeclaration),
              bufferSize = 1
            )
          )
          source.out ~> merge.in(n)
        }

        SourceShape(merge.out)
      })

      val result = mergedSources.map(_.bytes.utf8String).take(input.size).runWith(Sink.seq)

      result.futureValue.sorted shouldEqual input.sorted
    }

    "not fail on a fast producer and a slow consumer" in {
      val queueName = "amqp-conn-it-spec-simple-queue-2-" + System.currentTimeMillis()
      val queueDeclaration = QueueDeclaration(queueName)
      val amqpSource = AmqpSource(
        NamedQueueSourceSettings(conn, queueName).withDeclarations(queueDeclaration),
        bufferSize = 2
      )

      val amqpSink = AmqpSink.simple(
        AmqpSinkSettings(conn).withRoutingKey(queueName).withDeclarations(queueDeclaration)
      )

      val publisher = TestPublisher.probe[ByteString]()
      val subscriber = TestSubscriber.probe[IncomingMessage[ByteString]]()
      amqpSink.addAttributes(Attributes.inputBuffer(1, 1)).runWith(Source.fromPublisher(publisher))
      amqpSource.addAttributes(Attributes.inputBuffer(1, 1)).runWith(Sink.fromSubscriber(subscriber))

      // note that this essentially is testing rabbitmq just as much as it tests our sink and source
      publisher.ensureSubscription()
      subscriber.ensureSubscription()

      publisher.expectRequest() shouldEqual 1
      publisher.sendNext(ByteString("one"))

      publisher.expectRequest()
      publisher.sendNext(ByteString("two"))

      publisher.expectRequest()
      publisher.sendNext(ByteString("three"))

      publisher.expectRequest()
      publisher.sendNext(ByteString("four"))

      publisher.expectRequest()
      publisher.sendNext(ByteString("five"))

      subscriber.request(4)
      subscriber.expectNext().bytes.utf8String shouldEqual "one"
      subscriber.expectNext().bytes.utf8String shouldEqual "two"
      subscriber.expectNext().bytes.utf8String shouldEqual "three"
      subscriber.expectNext().bytes.utf8String shouldEqual "four"

      subscriber.request(1)
      subscriber.expectNext().bytes.utf8String shouldEqual "five"

      subscriber.cancel()
      publisher.sendComplete()
      succeed
    }

    "not ack messages unless they get consumed" in {
      val queueName = "amqp-conn-it-spec-simple-queue-2-" + System.currentTimeMillis()
      val queueDeclaration = QueueDeclaration(queueName)
      val amqpSource = AmqpSource(
        NamedQueueSourceSettings(conn, queueName).withDeclarations(queueDeclaration),
        bufferSize = 10
      )

      val amqpSink = AmqpSink.simple(
        AmqpSinkSettings(conn).withRoutingKey(queueName).withDeclarations(queueDeclaration)
      )

      val publisher = TestPublisher.probe[ByteString]()
      val subscriber = TestSubscriber.probe[IncomingMessage[ByteString]]()
      amqpSink.addAttributes(Attributes.inputBuffer(1, 1)).runWith(Source.fromPublisher(publisher))
      amqpSource.addAttributes(Attributes.inputBuffer(1, 1)).runWith(Sink.fromSubscriber(subscriber))

      // note that this essentially is testing rabbitmq just as much as it tests our sink and source
      publisher.ensureSubscription()
      subscriber.ensureSubscription()

      publisher.expectRequest() shouldEqual 1
      publisher.sendNext(ByteString("one"))

      publisher.expectRequest()
      publisher.sendNext(ByteString("two"))

      publisher.expectRequest()
      publisher.sendNext(ByteString("three"))

      publisher.expectRequest()
      publisher.sendNext(ByteString("four"))

      publisher.expectRequest()
      publisher.sendNext(ByteString("five"))

      // this should lead to all five being fetched into the buffer
      // but we just consume one before we cancel
      subscriber.request(1)
      subscriber.expectNext().bytes.utf8String shouldEqual "one"

      subscriber.cancel()
      publisher.sendComplete()

      val subscriber2 = TestSubscriber.probe[IncomingMessage[ByteString]]()
      amqpSource.addAttributes(Attributes.inputBuffer(1, 1)).runWith(Sink.fromSubscriber(subscriber2))

      subscriber2.ensureSubscription()
      subscriber2.request(4)
      subscriber2.expectNext().bytes.utf8String shouldEqual "two"
      subscriber2.expectNext().bytes.utf8String shouldEqual "three"
      subscriber2.expectNext().bytes.utf8String shouldEqual "four"
      subscriber2.expectNext().bytes.utf8String shouldEqual "five"

      subscriber2.cancel()
      succeed
    }

    "pub-sub from one source with multiple sinks" in {
      // with pubsub we arrange one exchange which the sink writes to
      // and then one queue for each source which subscribes to the
      // exchange - all this described by the declarations

      //#exchange-declaration
      val exchangeName = "amqp-conn-it-spec-pub-sub-" + System.currentTimeMillis()
      val exchangeDeclaration = ExchangeDeclaration(exchangeName, "fanout")
      //#exchange-declaration

      //#create-exchange-sink
      val amqpSink = AmqpSink.simple(
        AmqpSinkSettings(conn).withExchange(exchangeName).withDeclarations(exchangeDeclaration)
      )
      //#create-exchange-sink

      //#create-exchange-source
      val fanoutSize = 4

      val mergedSources = (0 until fanoutSize).foldLeft(Source.empty[(Int, String)]) {
        case (source, fanoutBranch) =>
          source.merge(
            AmqpSource(
              TemporaryQueueSourceSettings(
                conn,
                exchangeName
              ).withDeclarations(exchangeDeclaration),
              bufferSize = 1
            ).map(msg => (fanoutBranch, msg.bytes.utf8String))
          )
      }
      //#create-exchange-source

      val completion = Promise[Done]
      mergedSources.runWith(Sink.fold(Set.empty[Int]) {
        case (seen, (branch, element)) =>
          if (seen.size == fanoutSize) completion.trySuccess(Done)
          seen + branch
      })

      import system.dispatcher
      system.scheduler.scheduleOnce(5.seconds)(
        completion.tryFailure(new Error("Did not get at least one element from every fanout branch"))
      )

      Source.repeat("stuff").map(s => ByteString(s)).runWith(amqpSink)

      completion.future.futureValue shouldBe Done
    }
  }

  "The AMQP connection" should {

    "should" in {}

  }
}
