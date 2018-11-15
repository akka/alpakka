/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import akka.{Done, NotUsed}
import akka.stream.KillSwitches
import akka.stream.alpakka.amqp._
import akka.stream.alpakka.amqp.scaladsl._
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.testkit.scaladsl.TestSink
import akka.util.ByteString

import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
import scala.collection.immutable

/**
 * Needs a local running AMQP server on the default port with no password.
 */
class AmqpDocsSpec extends AmqpSpec {

  override implicit val patienceConfig = PatienceConfig(10.seconds)

  val businessLogic: CommittableReadResult => Future[CommittableReadResult] = Future.successful(_)

  "The AMQP Connectors" should {

    val connectionProvider = AmqpLocalConnectionProvider

    "publish and consume elements through a simple queue again in the same JVM" in assertAllStagesStopped {

      // use a list of host/port pairs where one is normally invalid, but
      // it should still work as expected,
      val connectionProvider =
        AmqpDetailsConnectionProvider("invalid", 5673).withHostsAndPorts(immutable.Seq("localhost" -> 5672))

      //#queue-declaration
      val queueName = "amqp-conn-it-spec-simple-queue-" + System.currentTimeMillis()
      val queueDeclaration = QueueDeclaration(queueName)
      //#queue-declaration

      //#create-sink
      val amqpSink: Sink[ByteString, Future[Done]] =
        AmqpSink.simple(
          AmqpWriteSettings(connectionProvider)
            .withRoutingKey(queueName)
            .withDeclaration(queueDeclaration)
        )

      val input = Vector("one", "two", "three", "four", "five")
      val writing: Future[Done] =
        Source(input)
          .map(s => ByteString(s))
          .runWith(amqpSink)
      //#create-sink
      writing.futureValue shouldEqual Done

      //#create-source
      val amqpSource: Source[ReadResult, NotUsed] =
        AmqpSource.atMostOnceSource(
          NamedQueueSourceSettings(connectionProvider, queueName)
            .withDeclaration(queueDeclaration)
            .withAckRequired(false),
          bufferSize = 10
        )

      val result: Future[immutable.Seq[ReadResult]] =
        amqpSource
          .take(input.size)
          .runWith(Sink.seq)
      //#create-source

      result.futureValue.map(_.bytes.utf8String) shouldEqual input
    }

    "publish via RPC and then consume through a simple queue again in the same JVM" in assertAllStagesStopped {

      val queueName = "amqp-conn-it-spec-rpc-queue-" + System.currentTimeMillis()
      val queueDeclaration = QueueDeclaration(queueName)

      val amqpSource = AmqpSource.atMostOnceSource(
        NamedQueueSourceSettings(connectionProvider, queueName),
        bufferSize = 1
      )

      val input = Vector("one", "two", "three", "four", "five")

      //#create-rpc-flow
      val amqpRpcFlow = AmqpRpcFlow.simple(
        AmqpWriteSettings(connectionProvider).withRoutingKey(queueName).withDeclaration(queueDeclaration)
      )

      val (rpcQueueF: Future[String], probe: TestSubscriber.Probe[ByteString]) = Source(input)
        .map(s => ByteString(s))
        .viaMat(amqpRpcFlow)(Keep.right)
        .toMat(TestSink.probe)(Keep.both)
        .run
      //#create-rpc-flow
      rpcQueueF.futureValue

      val amqpSink = AmqpSink.replyTo(
        AmqpReplyToSinkSettings(connectionProvider)
      )

      val sourceToSink = amqpSource
        .viaMat(KillSwitches.single)(Keep.right)
        .map(b => WriteMessage(b.bytes.concat(ByteString("a"))).withProperties(b.properties))
        .to(amqpSink)
        .run()

      probe.request(5).expectNextUnorderedN(input.map(s => ByteString(s.concat("a")))).expectComplete()
      sourceToSink.shutdown()
    }

    "correctly publish with successful confirms" in {
      val queueName = "amqp-conn-it-spec-publish-with-confirms-queue-" + System.currentTimeMillis()
      val queueDeclaration = QueueDeclaration(queueName)

      //#create-publish-flow
      val amqpPublishFlow = AmqpPublishFlow.simple[String](
        AmqpWriteSettings(connectionProvider)
          .withRoutingKey(queueName)
          .withDeclaration(queueDeclaration)
          .withPublishConfirm(confirmTimeout = 1000)
      )
      //#create-publish-flow

      val input = Vector("one", "two", "three", "four", "five")
      //#run-publish-flow
      val (_, probe) =
        Source(input)
          .map(s => (ByteString(s), s"$s-passThrough"))
          .viaMat(amqpPublishFlow)(Keep.right)
          .toMat(TestSink.probe)(Keep.both)
          .run
      //#run-publish-flow

      probe
        .request(input.length)
        .expectNextN(input.map(s => s"$s-passThrough"))
        .expectComplete()
    }

    "pub-sub from one source with multiple sinks" in assertAllStagesStopped {
      // with pubsub we arrange one exchange which the sink writes to
      // and then one queue for each source which subscribes to the
      // exchange - all this described by the declarations

      //#exchange-declaration
      val exchangeName = "amqp-conn-it-spec-pub-sub-" + System.currentTimeMillis()
      val exchangeDeclaration = ExchangeDeclaration(exchangeName, "fanout")
      //#exchange-declaration

      //#create-exchange-sink
      val amqpSink = AmqpSink.simple(
        AmqpWriteSettings(connectionProvider)
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
      val mergingFlow = mergedSources
        .viaMat(KillSwitches.single)(Keep.right)
        .to(Sink.fold(Set.empty[Int]) {
          case (seen, (branch, element)) =>
            if (seen.size == fanoutSize) completion.trySuccess(Done)
            seen + branch
        })
        .run()

      system.scheduler.scheduleOnce(5.seconds)(
        completion.tryFailure(new Error("Did not get at least one element from every fanout branch"))
      )

      val dataSender = Source
        .repeat("stuff")
        .viaMat(KillSwitches.single)(Keep.right)
        .map(s => ByteString(s))
        .to(amqpSink)
        .run()

      completion.future.futureValue shouldBe Done
      dataSender.shutdown()
      mergingFlow.shutdown()
    }

    "publish and consume elements through a simple queue again in the same JVM without autoAck" in assertAllStagesStopped {
      val queueName = "amqp-conn-it-spec-no-auto-ack-" + System.currentTimeMillis()
      val queueDeclaration = QueueDeclaration(queueName)

      val amqpSink = AmqpSink.simple(
        AmqpWriteSettings(connectionProvider)
          .withRoutingKey(queueName)
          .withDeclaration(queueDeclaration)
      )

      val input = Vector("one", "two", "three", "four", "five")
      Source(input).map(s => ByteString(s)).runWith(amqpSink).futureValue shouldEqual Done

      //#create-source-withoutautoack
      val amqpSource = AmqpSource.committableSource(
        NamedQueueSourceSettings(connectionProvider, queueName)
          .withDeclaration(queueDeclaration),
        bufferSize = 10
      )

      val result: Future[immutable.Seq[ReadResult]] = amqpSource
        .mapAsync(1)(businessLogic)
        .mapAsync(1)(cm => cm.ack().map(_ => cm.message))
        .take(input.size)
        .runWith(Sink.seq)
      //#create-source-withoutautoack

      result.futureValue.map(_.bytes.utf8String) shouldEqual input
    }

    "republish message without autoAck if nack is sent" in assertAllStagesStopped {

      val queueName = "amqp-conn-it-spec-no-auto-ack-nacked-" + System.currentTimeMillis()
      val queueDeclaration = QueueDeclaration(queueName)

      val amqpSink = AmqpSink.simple(
        AmqpWriteSettings(connectionProvider)
          .withRoutingKey(queueName)
          .withDeclaration(queueDeclaration)
      )

      val input = Vector("one", "two", "three", "four", "five")
      Source(input).map(s => ByteString(s)).runWith(amqpSink).futureValue shouldEqual Done

      val amqpSource = AmqpSource.committableSource(
        NamedQueueSourceSettings(connectionProvider, queueName).withDeclaration(queueDeclaration),
        bufferSize = 10
      )

      //#create-source-withoutautoack

      val nackedResults: Future[immutable.Seq[ReadResult]] = amqpSource
        .mapAsync(1)(businessLogic)
        .take(input.size)
        .mapAsync(1)(cm => cm.nack(multiple = false, requeue = true).map(_ => cm.message))
        .runWith(Sink.seq)
      //#create-source-withoutautoack

      Await.ready(nackedResults, 3.seconds)

      val result2 = amqpSource
        .mapAsync(1)(cm => cm.ack().map(_ => cm))
        .take(input.size)
        .runWith(Sink.seq)

      result2.futureValue.map(_.message.bytes.utf8String) shouldEqual input
    }
  }
}
