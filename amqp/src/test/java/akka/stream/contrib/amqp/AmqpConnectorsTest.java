/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.contrib.amqp;

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

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

/**
 * Needs a local running AMQP server on the default port with no password.
 */
public class AmqpConnectorsTest {

  static ActorSystem system;
  static Materializer materializer;

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
    Source.from(input).map(s -> ByteString.fromString(s)).runWith(amqpSink, materializer);
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

    final List<String> input = Arrays.asList("one", "two", "three", "four", "five");

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

    final CompletableFuture<Done> materialized = new CompletableFuture<>();
    final CompletionStage<List<Pair<Integer, String>>> result =  mergedSources
      .take(input.size() * fanoutSize)
      .mapMaterializedValue(matVal -> {
        materialized.complete(Done.getInstance());
        return matVal;
      })
      .runWith(Sink.seq(), materializer);

     // There is a race here if we don`t make sure the sources has declared their subscription queues and bindings
     // before we start writing to the exchange
     materialized.get(3, TimeUnit.SECONDS);
     Thread.sleep(200);

     Source.from(input).map(s -> ByteString.fromString(s)).runWith(amqpSink, materializer);

     final Set<Pair<Integer, String>> expectedResult = input
       .stream()
       .flatMap(str -> IntStream.range(0, fanoutSize).boxed().map(i -> Pair.create(i, str)))
       .collect(Collectors.toSet());

     assertEquals(expectedResult, result.toCompletableFuture().get(10, TimeUnit.SECONDS).stream().collect(Collectors.toSet()));
  }

}
