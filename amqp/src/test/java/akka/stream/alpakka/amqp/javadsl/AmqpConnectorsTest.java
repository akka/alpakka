/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.amqp.javadsl;

import akka.stream.alpakka.amqp.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.*;
import akka.stream.javadsl.*;
import akka.testkit.JavaTestKit;
import akka.util.ByteString;
import scala.concurrent.duration.Duration;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

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

    //#create-sink
    final Sink<ByteString, NotUsed> amqpSink = AmqpSink.createSimple(
      AmqpSinkSettings.create(DefaultAmqpConnection.getInstance())
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
  public void publishFanoutAndConsume() throws Exception {
    //#exchange-declaration
    final String exchangeName = "amqp-conn-it-spec-pub-sub" + System.currentTimeMillis();
    final ExchangeDeclaration exchangeDeclaration = ExchangeDeclaration.create(exchangeName, "fanout");
    //#exchange-declaration

    //#create-exchange-sink
    final Sink<ByteString, NotUsed> amqpSink = AmqpSink.createSimple(
      AmqpSinkSettings.create(DefaultAmqpConnection.getInstance())
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
