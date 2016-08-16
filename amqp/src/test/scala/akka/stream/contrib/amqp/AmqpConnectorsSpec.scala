/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib.amqp

import java.util.concurrent.atomic.AtomicInteger

import akka.Done
import akka.stream.javadsl.SinkQueueWithCancel
import akka.stream._
import akka.stream.scaladsl.{ Flow, GraphDSL, Keep, Merge, Sink, Source }
import akka.stream.testkit.{ TestPublisher, TestSubscriber }
import akka.util.ByteString
import akka.testkit.TestKit.awaitCond

import scala.concurrent.Promise
import scala.concurrent.duration._

class AmqpConnectorsSpec extends AmqpSpec {

  override implicit val patienceConfig = PatienceConfig(10.seconds)

  var usedQueues = Seq.empty[String]
  var usedExchanges = Seq.empty[String]

  "The AMQP Connectors" should {

    "publish and consume elements through a simple queue" in {
      val queueName = "amqp-conn-it-spec-simple-queue-" + System.currentTimeMillis()
      usedQueues = usedQueues :+ queueName
      val queueDeclaration = QueueDeclaration(queueName)
      val amqpSource = AmqpSource(
        NamedQueueSourceSettings(
          DefaultAmqpConnection,
          queueName,
          List(queueDeclaration)
        ), bufferSize = 10
      )

      val amqpSink = AmqpSink.simple(
        AmqpSinkSettings(
          DefaultAmqpConnection,
          None,
          Some(queueName),
          List(queueDeclaration)
        )
      )

      val input = Vector("one", "two", "three", "four", "five")
      Source(input).map(s => ByteString(s)).runWith(amqpSink)

      val result = amqpSource.map(_.bytes.utf8String).take(input.size).runWith(Sink.seq)

      result.futureValue shouldEqual input
    }

    "publish and consume elements through a simple queue again in the same JVM" in {
      val queueName = "amqp-conn-it-spec-simple-queue-2-" + System.currentTimeMillis()
      usedQueues = usedQueues :+ queueName
      val queueDeclaration = QueueDeclaration(queueName)
      val amqpSource = AmqpSource(
        NamedQueueSourceSettings(
          DefaultAmqpConnection,
          queueName,
          List(queueDeclaration)
        ), bufferSize = 10
      )

      val amqpSink = AmqpSink.simple(
        AmqpSinkSettings(
          DefaultAmqpConnection,
          exchange = None,
          routingKey = Some(queueName),
          List(queueDeclaration)
        )
      )

      val input = Vector("one", "two", "three", "four", "five")
      Source(input).map(s => ByteString(s)).runWith(amqpSink)

      val result = amqpSource.map(_.bytes.utf8String).take(input.size).runWith(Sink.seq)

      result.futureValue shouldEqual input
    }

    "publish from one source and consume elements with multiple sinks" in {
      val queueName = "amqp-conn-it-spec-work-queues-" + System.currentTimeMillis()
      usedQueues = usedQueues :+ queueName
      val queueDeclaration = QueueDeclaration(queueName)
      val amqpSink = AmqpSink.simple(
        AmqpSinkSettings(
          DefaultAmqpConnection,
          exchange = None,
          routingKey = Some(queueName),
          List(queueDeclaration)
        )
      )

      val input = Vector("one", "two", "three", "four", "five")
      Source(input).map(s => ByteString(s)).runWith(amqpSink)

      val mergedSources = Source.fromGraph(GraphDSL.create() { implicit b =>
        import GraphDSL.Implicits._
        val count = 3
        val merge = b.add(Merge[IncomingMessage](count))
        for (n <- 0 until count) {
          val source = b.add(AmqpSource(NamedQueueSourceSettings(
            DefaultAmqpConnection,
            queueName,
            List(queueDeclaration)
          ), bufferSize = 1))
          source.out ~> merge.in(n)
        }

        SourceShape(merge.out)
      })

      val result = mergedSources.map(_.bytes.utf8String).take(input.size).runWith(Sink.seq)

      result.futureValue.sorted shouldEqual input.sorted
    }

    "not fail on a fast producer and a slow consumer" in {
      val queueName = "amqp-conn-it-spec-simple-queue-2-" + System.currentTimeMillis()
      usedQueues = usedQueues :+ queueName
      val queueDeclaration = QueueDeclaration(queueName)
      val amqpSource = AmqpSource(
        NamedQueueSourceSettings(
          DefaultAmqpConnection,
          queueName,
          List(queueDeclaration)
        ), bufferSize = 2
      )

      val amqpSink = AmqpSink.simple(
        AmqpSinkSettings(
          DefaultAmqpConnection,
          exchange = None,
          routingKey = Some(queueName),
          List(queueDeclaration)
        )
      )

      val publisher = TestPublisher.probe[ByteString]()
      val subscriber = TestSubscriber.probe[IncomingMessage]()
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

    }

    "not ack messages unless they get consumed" in {
      val queueName = "amqp-conn-it-spec-simple-queue-2-" + System.currentTimeMillis()
      usedQueues = usedQueues :+ queueName
      val queueDeclaration = QueueDeclaration(queueName)
      val amqpSource = AmqpSource(
        NamedQueueSourceSettings(
          DefaultAmqpConnection,
          queueName,
          List(queueDeclaration)
        ), bufferSize = 10
      )

      val amqpSink = AmqpSink.simple(
        AmqpSinkSettings(
          DefaultAmqpConnection,
          exchange = None,
          routingKey = Some(queueName),
          List(queueDeclaration)
        )
      )

      val publisher = TestPublisher.probe[ByteString]()
      val subscriber = TestSubscriber.probe[IncomingMessage]()
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

      val subscriber2 = TestSubscriber.probe[IncomingMessage]()
      amqpSource.addAttributes(Attributes.inputBuffer(1, 1)).runWith(Sink.fromSubscriber(subscriber2))

      subscriber2.ensureSubscription()
      subscriber2.request(4)
      subscriber2.expectNext().bytes.utf8String shouldEqual "two"
      subscriber2.expectNext().bytes.utf8String shouldEqual "three"
      subscriber2.expectNext().bytes.utf8String shouldEqual "four"
      subscriber2.expectNext().bytes.utf8String shouldEqual "five"

      subscriber2.cancel()

    }

    "pub-sub from one source with multiple sinks" in {
      // with pubsub we arrange one exchange which the sink writes to
      // and then one queue for each source which subscribes to the
      // exchange - all this described by the declarations
      val exchangeName = "amqp-conn-it-spec-pub-sub-" + System.currentTimeMillis()
      usedExchanges = usedExchanges :+ exchangeName

      val exchangeDeclaration = ExchangeDeclaration(exchangeName, "fanout")

      val amqpSink = AmqpSink.simple(
        AmqpSinkSettings(
          DefaultAmqpConnection,
          Some(exchangeName),
          None,
          List(exchangeDeclaration)
        )
      )

      val count = 4
      val mergedSources = Source.fromGraph(GraphDSL.create() { implicit b =>
        import GraphDSL.Implicits._
        val merge = b.add(Merge[(IncomingMessage, Int)](count))
        for (n <- 0 until count) {
          val declarations = List(
            exchangeDeclaration
          )
          val source = b.add(AmqpSource(
            TemporaryQueueSourceSettings(
              DefaultAmqpConnection,
              exchangeName,
              List(exchangeDeclaration)
            ),
            bufferSize = 1
          ).map(msg => (msg, n)))
          source.out ~> merge.in(n)
        }

        SourceShape(merge.out)
      })

      val materialized = Promise[Done]()
      val futureResult = mergedSources.map(t => (t._2, t._1.bytes.utf8String))
        .takeWithin(10.seconds)
        .mapMaterializedValue { n =>
          materialized.success(Done)
          n
        }.runWith(Sink.seq)

      // There is a race here if we don`t make sure the sources has declared their subscription queues and bindings
      // before we start writing to the exchange
      materialized.future.futureValue
      Thread.sleep(200)

      val input = Vector("one", "two", "three", "four", "five")
      Source(input).map(s => ByteString(s)).runWith(amqpSink)

      val expectedOutput = input.flatMap(string => (0 until 4).map(n => (n, string))).toSet
      futureResult.futureValue.toSet shouldEqual expectedOutput
    }
  }

  override protected def afterAll(): Unit = {
    println("Cleaning up queues and exchanges")
    import sys.process._
    // delete all used queues
    usedQueues.foreach { queueName =>
      s"rabbitmqadmin delete queue name=$queueName".!
    }
    usedExchanges.foreach { exchangeName =>
      s"rabbitmqadmin delete exchange name=$exchangeName".!
    }
    super.afterAll()
  }
}
