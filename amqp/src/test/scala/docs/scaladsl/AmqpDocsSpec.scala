/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import akka.Done
import akka.stream.alpakka.amqp._
import akka.stream.alpakka.amqp.scaladsl.{AmqpRpcFlow, AmqpSink, AmqpSource}
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.testkit.scaladsl.TestSink
import akka.util.ByteString

import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}
import scala.collection.immutable

/**
 * Needs a local running AMQP server on the default port with no password.
 */
class AmqpDocsSpec extends AmqpSpec {

  override implicit val patienceConfig = PatienceConfig(10.seconds)

  "The AMQP Connectors" should {

    val connectionProvider = AmqpLocalConnectionProvider

    "publish and consume elements through a simple queue again in the same JVM" in {

      // use a list of host/port pairs where one is normally invalid, but
      // it should still work as expected,
      val connectionProvider =
        AmqpDetailsConnectionProvider(List(("invalid", 5673))).withHostsAndPorts(("localhost", 5672))

      //#queue-declaration
      val queueName = "amqp-conn-it-spec-simple-queue-" + System.currentTimeMillis()
      val queueDeclaration = QueueDeclaration(queueName)
      //#queue-declaration

      //#create-sink
      val amqpSink = AmqpSink.simple(
        AmqpSinkSettings(connectionProvider)
          .withRoutingKey(queueName)
          .withDeclaration(queueDeclaration)
      )
      //#create-sink

      //#create-source
      val amqpSource = AmqpSource.atMostOnceSource(
        NamedQueueSourceSettings(connectionProvider, queueName).withDeclaration(queueDeclaration),
        bufferSize = 10
      )
      //#create-source

      //#run-sink
      val input = Vector("one", "two", "three", "four", "five")
      Source(input).map(s => ByteString(s)).runWith(amqpSink).futureValue shouldEqual Done
      //#run-sink

      //#run-source
      val result = amqpSource.take(input.size).runWith(Sink.seq)
      //#run-source

      result.futureValue.map(_.bytes.utf8String) shouldEqual input
    }

    "publish via RPC and then consume through a simple queue again in the same JVM" in {

      val queueName = "amqp-conn-it-spec-rpc-queue-" + System.currentTimeMillis()
      val queueDeclaration = QueueDeclaration(queueName)

      //#create-rpc-flow
      val amqpRpcFlow = AmqpRpcFlow.simple(
        AmqpSinkSettings(connectionProvider).withRoutingKey(queueName).withDeclaration(queueDeclaration)
      )
      //#create-rpc-flow

      val amqpSource = AmqpSource.atMostOnceSource(
        NamedQueueSourceSettings(connectionProvider, queueName),
        bufferSize = 1
      )

      val input = Vector("one", "two", "three", "four", "five")
      //#run-rpc-flow
      val (rpcQueueF, probe) = Source(input)
        .map(s => ByteString(s))
        .viaMat(amqpRpcFlow)(Keep.right)
        .toMat(TestSink.probe)(Keep.both)
        .run
      //#run-rpc-flow
      rpcQueueF.futureValue

      val amqpSink = AmqpSink.replyTo(
        AmqpReplyToSinkSettings(connectionProvider)
      )

      amqpSource
        .map(b => OutgoingMessage(b.bytes.concat(ByteString("a")), false, false).withProperties(b.properties))
        .runWith(amqpSink)

      probe.request(5).expectNextUnorderedN(input.map(s => ByteString(s.concat("a")))).expectComplete()
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
        AmqpSinkSettings(connectionProvider)
          .withExchange(exchangeName)
          .withDeclaration(exchangeDeclaration)
      )
      //#create-exchange-sink

      //#create-exchange-source
      val fanoutSize = 4

      val mergedSources = (0 until fanoutSize).foldLeft(Source.empty[(Int, String)]) {
        case (source, fanoutBranch) =>
          source.merge(
            AmqpSource
              .atMostOnceSource(
                TemporaryQueueSourceSettings(
                  connectionProvider,
                  exchangeName
                ).withDeclaration(exchangeDeclaration),
                bufferSize = 1
              )
              .map(msg => (fanoutBranch, msg.bytes.utf8String))
          )
      }
      //#create-exchange-source

      val completion = Promise[Done]
      mergedSources.runWith(Sink.fold(Set.empty[Int]) {
        case (seen, (branch, element)) =>
          if (seen.size == fanoutSize) completion.trySuccess(Done)
          seen + branch
      })

      system.scheduler.scheduleOnce(5.seconds)(
        completion.tryFailure(new Error("Did not get at least one element from every fanout branch"))
      )

      Source.repeat("stuff").map(s => ByteString(s)).runWith(amqpSink)

      completion.future.futureValue shouldBe Done
    }

    "publish and consume elements through a simple queue again in the same JVM without autoAck" in {
      val queueName = "amqp-conn-it-spec-no-auto-ack-" + System.currentTimeMillis()
      val queueDeclaration = QueueDeclaration(queueName)

      val amqpSink = AmqpSink.simple(
        AmqpSinkSettings(connectionProvider)
          .withRoutingKey(queueName)
          .withDeclaration(queueDeclaration)
      )

      //#create-source-withoutautoack
      val amqpSource = AmqpSource.committableSource(
        NamedQueueSourceSettings(connectionProvider, queueName).withDeclaration(queueDeclaration),
        bufferSize = 10
      )
      //#create-source-withoutautoack

      val input = Vector("one", "two", "three", "four", "five")
      Source(input).map(s => ByteString(s)).runWith(amqpSink).futureValue shouldEqual Done

      //#run-source-withoutautoack
      val result = amqpSource
        .mapAsync(1)(cm => cm.ack().map(_ => cm))
        .take(input.size)
        .runWith(Sink.seq)
      //#run-source-withoutautoack

      result.futureValue.map(_.message.bytes.utf8String) shouldEqual input
    }

    "republish message without autoAck if nack is sent" in {

      val queueName = "amqp-conn-it-spec-no-auto-ack-nacked-" + System.currentTimeMillis()
      val queueDeclaration = QueueDeclaration(queueName)

      val amqpSink = AmqpSink.simple(
        AmqpSinkSettings(connectionProvider)
          .withRoutingKey(queueName)
          .withDeclaration(queueDeclaration)
      )

      val input = Vector("one", "two", "three", "four", "five")
      Source(input).map(s => ByteString(s)).runWith(amqpSink).futureValue shouldEqual Done

      val amqpSource = AmqpSource.committableSource(
        NamedQueueSourceSettings(connectionProvider, queueName).withDeclaration(queueDeclaration),
        bufferSize = 10
      )

      //#run-source-withoutautoack-and-nack
      val result1 = amqpSource
        .take(input.size)
        .mapAsync(1)(cm => cm.nack().map(_ => cm))
        .runWith(Sink.seq)
      //#run-source-withoutautoack-and-nack

      Await.ready(result1, 3.seconds)

      val result2 = amqpSource
        .mapAsync(1)(cm => cm.ack().map(_ => cm))
        .take(input.size)
        .runWith(Sink.seq)

      result2.futureValue.map(_.message.bytes.utf8String) shouldEqual input
    }
  }
}
