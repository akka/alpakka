/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.amqp.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.amqp.*;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.testkit.TestSubscriber;
import akka.stream.testkit.javadsl.TestSink;
import akka.testkit.javadsl.TestKit;
import akka.util.ByteString;
import com.rabbitmq.client.AuthenticationFailureException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.collection.JavaConverters;
import scala.concurrent.duration.Duration;

import java.net.ConnectException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/** Needs a local running AMQP server on the default port with no password. */
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
    TestKit.shutdownActorSystem(system);
  }

  private AmqpConnectionProvider connectionProvider = AmqpLocalConnectionProvider.getInstance();

  public void publishAndConsume() throws Exception {
    // see AmqpDocsTest
  }

  @Test(expected = ConnectException.class)
  public void throwIfCanNotConnect() throws Throwable {
    final String queueName = "amqp-conn-it-spec-simple-queue-" + System.currentTimeMillis();
    final QueueDeclaration queueDeclaration = QueueDeclaration.create(queueName);

    @SuppressWarnings("unchecked")
    AmqpDetailsConnectionProvider connectionProvider =
        AmqpDetailsConnectionProvider.create("localhost", 5673);

    final Sink<ByteString, CompletionStage<Done>> amqpSink =
        AmqpSink.createSimple(
            AmqpSinkSettings.create(connectionProvider)
                .withRoutingKey(queueName)
                .withDeclarations(Collections.singletonList(queueDeclaration)));

    final List<String> input = Arrays.asList("one", "two", "three", "four", "five");
    final CompletionStage<Done> result =
        Source.from(input).map(ByteString::fromString).runWith(amqpSink, materializer);

    try {
      result.toCompletableFuture().get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  @Test(expected = AuthenticationFailureException.class)
  public void throwWithWrongCredentials() throws Throwable {
    final String queueName = "amqp-conn-it-spec-simple-queue-" + System.currentTimeMillis();
    final QueueDeclaration queueDeclaration = QueueDeclaration.create(queueName);

    @SuppressWarnings("unchecked")
    AmqpDetailsConnectionProvider connectionProvider =
        AmqpDetailsConnectionProvider.create("invalid", 5673)
            .withHostsAndPorts(Pair.create("localhost", 5672))
            .withCredentials(AmqpCredentials.create("guest", "guest1"));

    final Sink<ByteString, CompletionStage<Done>> amqpSink =
        AmqpSink.createSimple(
            AmqpSinkSettings.create(connectionProvider)
                .withRoutingKey(queueName)
                .withDeclarations(Collections.singletonList(queueDeclaration)));

    final List<String> input = Arrays.asList("one", "two", "three", "four", "five");
    final CompletionStage<Done> result =
        Source.from(input).map(ByteString::fromString).runWith(amqpSink, materializer);

    try {
      result.toCompletableFuture().get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
    // assertEquals(input, result.toCompletableFuture().get(3, TimeUnit.SECONDS).stream().map(m ->
    // m.bytes().utf8String()).collect(Collectors.toList()));
  }

  public void publishAndConsumeRpc() throws Exception {
    // see AmqpDocsTest
  }

  public void publishFanoutAndConsume() throws Exception {
    // see AmqpDocsTest
  }

  public void publishAndConsumeWithoutAutoAck() throws Exception {
    // see AmqpDocsTest
  }

  @Test
  public void publishAndConsumeRpcWithoutAutoAck() throws Exception {

    final String queueName = "amqp-conn-it-spec-rpc-queue-" + System.currentTimeMillis();
    final QueueDeclaration queueDeclaration = QueueDeclaration.create(queueName);

    final List<String> input = Arrays.asList("one", "two", "three", "four", "five");

    final Flow<OutgoingMessage, CommittableIncomingMessage, CompletionStage<String>> ampqRpcFlow =
        AmqpRpcFlow.committableFlow(
            AmqpSinkSettings.create(connectionProvider)
                .withRoutingKey(queueName)
                .withDeclarations(Collections.singletonList(queueDeclaration)),
            10,
            1);
    Pair<CompletionStage<String>, TestSubscriber.Probe<IncomingMessage>> result =
        Source.from(input)
            .map(ByteString::fromString)
            .map(bytes -> OutgoingMessage.create(bytes, false, false))
            .viaMat(ampqRpcFlow, Keep.right())
            .mapAsync(1, cm -> cm.ack(false).thenApply(unused -> cm.message()))
            .toMat(TestSink.probe(system), Keep.both())
            .run(materializer);

    result.first().toCompletableFuture().get(5, TimeUnit.SECONDS);

    Sink<OutgoingMessage, CompletionStage<Done>> amqpSink =
        AmqpSink.createReplyTo(AmqpReplyToSinkSettings.create(connectionProvider));

    final Source<IncomingMessage, NotUsed> amqpSource =
        AmqpSource.atMostOnceSource(
            NamedQueueSourceSettings.create(connectionProvider, queueName)
                .withDeclarations(Collections.singletonList(queueDeclaration)),
            1);

    amqpSource
        .map(b -> OutgoingMessage.create(b.bytes(), false, false).withProperties(b.properties()))
        .runWith(amqpSink, materializer);

    List<IncomingMessage> probeResult =
        JavaConverters.seqAsJavaListConverter(
                result.second().toStrict(Duration.create(3, TimeUnit.SECONDS)))
            .asJava();
    assertEquals(
        probeResult.stream().map(s -> s.bytes().utf8String()).collect(Collectors.toList()), input);
  }

  @Test
  public void republishMessageWithoutAutoAckIfNacked() throws Exception {
    // see AmqpDocsTest
  }

  @Test
  public void keepConnectionOpenIfDownstreamClosesAndThereArePendingAcks() throws Exception {
    final String queueName = "amqp-conn-it-spec-simple-queue-" + System.currentTimeMillis();
    final QueueDeclaration queueDeclaration = QueueDeclaration.create(queueName);

    final Sink<ByteString, CompletionStage<Done>> amqpSink =
        AmqpSink.createSimple(
            AmqpSinkSettings.create(connectionProvider)
                .withRoutingKey(queueName)
                .withDeclarations(Collections.singletonList(queueDeclaration)));

    final Integer bufferSize = 10;
    final Source<CommittableIncomingMessage, NotUsed> amqpSource =
        AmqpSource.committableSource(
            NamedQueueSourceSettings.create(connectionProvider, queueName)
                .withDeclarations(Collections.singletonList(queueDeclaration)),
            bufferSize);

    final List<String> input = Arrays.asList("one", "two", "three", "four", "five");
    Source.from(input)
        .map(ByteString::fromString)
        .runWith(amqpSink, materializer)
        .toCompletableFuture()
        .get(3, TimeUnit.SECONDS);

    final CompletionStage<List<CommittableIncomingMessage>> result =
        amqpSource.take(input.size()).runWith(Sink.seq(), materializer);

    result
        .toCompletableFuture()
        .get(3, TimeUnit.SECONDS)
        .stream()
        .map(
            cm -> {
              try {
                cm.ack(false).toCompletableFuture().get(3, TimeUnit.SECONDS);
              } catch (Exception e) {
                assertEquals(e.getMessage(), false, true);
              }
              return true;
            });
  }

  @Test
  public void setRoutingKeyPerMessageAndConsumeThemInTheSameJVM() throws Exception {
    final String exchangeName = "amqp.topic." + System.currentTimeMillis();
    final ExchangeDeclaration exchangeDeclaration =
        ExchangeDeclaration.create(exchangeName, "topic");
    final String queueName = "amqp-conn-it-spec-simple-queue-" + System.currentTimeMillis();
    final QueueDeclaration queueDeclaration = QueueDeclaration.create(queueName);
    final BindingDeclaration bindingDeclaration =
        BindingDeclaration.create(queueName, exchangeName).withRoutingKey("key.*");

    final Sink<OutgoingMessage, CompletionStage<Done>> amqpSink =
        AmqpSink.create(
            AmqpSinkSettings.create(connectionProvider)
                .withExchange(exchangeName)
                .withDeclarations(
                    Arrays.asList(exchangeDeclaration, queueDeclaration, bindingDeclaration)));

    final Integer bufferSize = 10;
    final Source<IncomingMessage, NotUsed> amqpSource =
        AmqpSource.atMostOnceSource(
            NamedQueueSourceSettings.create(connectionProvider, queueName)
                .withDeclarations(
                    Arrays.asList(exchangeDeclaration, queueDeclaration, bindingDeclaration)),
            bufferSize);

    final List<String> input = Arrays.asList("one", "two", "three", "four", "five");
    final List<String> routingKeys =
        input.stream().map(s -> "key." + s).collect(Collectors.toList());
    Source.from(input)
        .map(
            s ->
                OutgoingMessage.create(ByteString.fromString(s), false, false)
                    .withRoutingKey("key." + s))
        .runWith(amqpSink, materializer);

    final List<IncomingMessage> result =
        amqpSource
            .take(input.size())
            .runWith(Sink.seq(), materializer)
            .toCompletableFuture()
            .get(3, TimeUnit.SECONDS);

    assertEquals(
        routingKeys,
        result.stream().map(m -> m.envelope().getRoutingKey()).collect(Collectors.toList()));
    assertEquals(
        input, result.stream().map(m -> m.bytes().utf8String()).collect(Collectors.toList()));
  }
}
