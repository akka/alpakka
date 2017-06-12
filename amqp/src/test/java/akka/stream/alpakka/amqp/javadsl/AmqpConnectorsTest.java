/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.amqp.javadsl;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.amqp.*;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.testkit.TestSubscriber;
import akka.stream.testkit.javadsl.TestSink;
import akka.testkit.JavaTestKit;
import akka.util.ByteString;
import scala.Some;
import scala.concurrent.duration.Duration;

/**
 * Needs a local running AMQP server on the default port with no password.
 */
public class AmqpConnectorsTest {

  private static ActorSystem system;
  private static Materializer materializer;

  @BeforeClass
  public static void setup() {
    system = ActorSystem.create();
    materializer = ActorMaterializer.create(system);
  }

  @AfterClass
  public static void teardown() {
    JavaTestKit.shutdownActorSystem(system);
  }

  @Test
  public void publishAndConsume() throws Exception {
    //#queue-declaration
    final String queueName = "amqp-conn-it-spec-simple-queue-" + System.currentTimeMillis();
    final QueueDeclaration queueDeclaration = QueueDeclaration.create(queueName);
    //#queue-declaration

    @SuppressWarnings("unchecked")
    AmqpConnectionDetails amqpConnectionDetails = AmqpConnectionDetails.create("invalid", 5673)
          .withHostsAndPorts(Pair.create("localhost", 5672), Pair.create("localhost", 5674));

    //#create-sink
    final Sink<ByteString, CompletionStage<Done>> amqpSink = AmqpSink.createSimple(
      AmqpSinkSettings.create(amqpConnectionDetails)
        .withRoutingKey(queueName)
        .withDeclarations(queueDeclaration)
    );
    //#create-sink

    //#create-source
    final Integer bufferSize = 10;
    final Source<IncomingMessage, NotUsed> amqpSource = AmqpSource.create(
      NamedQueueSourceSettings.create(
        DefaultAmqpConnection.getInstance(),
        queueName
      ).withDeclarations(queueDeclaration),
      bufferSize
    );
    //#create-source

    //#run-sink
    final List<String> input = Arrays.asList("one", "two", "three", "four", "five");
    Source.from(input).map(ByteString::fromString).runWith(amqpSink, materializer);
    //#run-sink

    //#run-source
    final CompletionStage<List<String>> result =
      amqpSource.map(m -> m.bytes().utf8String()).take(input.size()).runWith(Sink.seq(), materializer);
    //#run-source

    assertEquals(input, result.toCompletableFuture().get(3, TimeUnit.SECONDS));
  }


  @Test
  public void publishAndConsumeRpc() throws Exception {

    final String queueName = "amqp-conn-it-spec-rpc-queue-" + System.currentTimeMillis();
    final QueueDeclaration queueDeclaration = QueueDeclaration.create(queueName);

    //#create-rpc-flow
    final Flow<ByteString,ByteString, CompletionStage<String>> ampqRpcFlow = AmqpRpcFlow.createSimple(
        AmqpSinkSettings.create().withRoutingKey(queueName).withDeclarations(queueDeclaration), 1);
    //#create-rpc-flow

    final Integer bufferSize = 10;
    final Source<IncomingMessage, NotUsed> amqpSource = AmqpSource.create(
        NamedQueueSourceSettings.create(
            DefaultAmqpConnection.getInstance(),
            queueName
        ).withDeclarations(queueDeclaration),
        bufferSize
    );

    //#run-rpc-flow
    final List<String> input = Arrays.asList("one", "two", "three", "four", "five");
    TestSubscriber.Probe<ByteString> probe =
        Source.from(input)
            .map(ByteString::fromString)
            .via(ampqRpcFlow)
            .runWith(TestSink.probe(system), materializer);
    //#run-rpc-flow

    Sink<OutgoingMessage, CompletionStage<Done>> amqpSink = AmqpSink.createReplyTo(
        AmqpReplyToSinkSettings.create(DefaultAmqpConnection.getInstance())
    );

    amqpSource.map(b ->
        new OutgoingMessage(b.bytes().concat(ByteString.fromString("a")), false, false, Some.apply(b.properties()))
      ).runWith(amqpSink, materializer);

    probe.request(5)
      .expectNextUnordered(
          ByteString.fromString("onea"),
          ByteString.fromString("twoa"),
          ByteString.fromString("threea"),
          ByteString.fromString("foura"),
          ByteString.fromString("fivea")
      ).expectComplete();

    final CompletionStage<List<String>> result =
        amqpSource.map(m -> m.bytes().utf8String()).take(input.size()).runWith(Sink.seq(), materializer);

  }

  @Test
  public void publishFanoutAndConsume() throws Exception {
    //#exchange-declaration
    final String exchangeName = "amqp-conn-it-spec-pub-sub" + System.currentTimeMillis();
    final ExchangeDeclaration exchangeDeclaration = ExchangeDeclaration.create(exchangeName, "fanout");
    //#exchange-declaration

    //#create-exchange-sink
    final Sink<ByteString, CompletionStage<Done>> amqpSink = AmqpSink.createSimple(
      AmqpSinkSettings.create()
        .withExchange(exchangeName)
        .withDeclarations(exchangeDeclaration)
    );
    //#create-exchange-sink

    //#create-exchange-source
    final Integer fanoutSize = 4;
    final Integer bufferSize = 1;

    Source<Pair<Integer, String>, NotUsed> mergedSources = Source.empty();
    for (Integer i = 0; i < fanoutSize; i++) {
      final Integer fanoutBranch = i;
      mergedSources = mergedSources.merge(
        AmqpSource.create(
          TemporaryQueueSourceSettings.create(
            DefaultAmqpConnection.getInstance(),
            exchangeName
          ).withDeclarations(exchangeDeclaration),
          bufferSize
        )
        .map(msg -> Pair.create(fanoutBranch, msg.bytes().utf8String()))
      );
    }
    //#create-exchange-source

    final CompletableFuture<Done> completion = new CompletableFuture<>();
    mergedSources
      .runWith(Sink.fold(new HashSet<Integer>(), (seen, branchElem) -> {
        if (seen.size() == fanoutSize) {
          completion.complete(Done.getInstance());
        }
        seen.add(branchElem.first());
        return seen;
      }), materializer);

    system.scheduler().scheduleOnce(
      Duration.create(5, TimeUnit.SECONDS),
      () -> completion.completeExceptionally(new Error("Did not get at least one element from every fanout branch")),
      system.dispatcher());

     Source.repeat("stuff").map(ByteString::fromString).runWith(amqpSink, materializer);

     assertEquals(Done.getInstance(), completion.get(10, TimeUnit.SECONDS));
  }

}
