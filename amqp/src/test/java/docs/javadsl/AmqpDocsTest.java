/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.KillSwitches;
import akka.stream.Materializer;
import akka.stream.UniqueKillSwitch;
import akka.stream.alpakka.amqp.*;
import akka.stream.alpakka.amqp.javadsl.AmqpFlow;
import akka.stream.alpakka.amqp.javadsl.AmqpRpcFlow;
import akka.stream.alpakka.amqp.javadsl.AmqpSink;
import akka.stream.alpakka.amqp.javadsl.AmqpSource;
import akka.stream.alpakka.amqp.javadsl.CommittableReadResult;
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.testkit.TestSubscriber;
import akka.stream.testkit.javadsl.StreamTestKit;
import akka.stream.testkit.javadsl.TestSink;
import akka.testkit.javadsl.TestKit;
import akka.util.ByteString;

import org.junit.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/** Needs a local running AMQP server on the default port with no password. */
public class AmqpDocsTest {

  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  private static ActorSystem system;

  @BeforeClass
  public static void setup() {
    system = ActorSystem.create();
  }

  @AfterClass
  public static void teardown() {
    TestKit.shutdownActorSystem(system);
  }

  @After
  public void checkForStageLeaks() {
    StreamTestKit.assertAllStagesStopped(Materializer.matFromSystem(system));
  }

  private AmqpConnectionProvider connectionProvider = AmqpLocalConnectionProvider.getInstance();

  @Test
  public void publishAndConsume() throws Exception {
    // #queue-declaration
    final String queueName = "amqp-conn-it-test-simple-queue-" + System.currentTimeMillis();
    final QueueDeclaration queueDeclaration = QueueDeclaration.create(queueName);
    // #queue-declaration

    @SuppressWarnings("unchecked")
    AmqpDetailsConnectionProvider connectionProvider =
        AmqpDetailsConnectionProvider.create("invalid", 5673)
            .withHostsAndPorts(
                Arrays.asList(Pair.create("localhost", 5672), Pair.create("localhost", 5674)));

    // #create-sink
    final Sink<ByteString, CompletionStage<Done>> amqpSink =
        AmqpSink.createSimple(
            AmqpWriteSettings.create(connectionProvider)
                .withRoutingKey(queueName)
                .withDeclaration(queueDeclaration));

    final List<String> input = Arrays.asList("one", "two", "three", "four", "five");
    CompletionStage<Done> writing =
        Source.from(input).map(ByteString::fromString).runWith(amqpSink, system);
    // #create-sink
    writing.toCompletableFuture().get(3, TimeUnit.SECONDS);

    // #create-source
    final Integer bufferSize = 10;
    final Source<ReadResult, NotUsed> amqpSource =
        AmqpSource.atMostOnceSource(
            NamedQueueSourceSettings.create(connectionProvider, queueName)
                .withDeclaration(queueDeclaration)
                .withAckRequired(false),
            bufferSize);

    final CompletionStage<List<ReadResult>> result =
        amqpSource.take(input.size()).runWith(Sink.seq(), system);
    // #create-source

    assertEquals(
        input,
        result.toCompletableFuture().get(3, TimeUnit.SECONDS).stream()
            .map(m -> m.bytes().utf8String())
            .collect(Collectors.toList()));
  }

  @Test
  public void publishAndConsumeRpc() throws Exception {

    final String queueName = "amqp-conn-it-test-rpc-queue-" + System.currentTimeMillis();
    final QueueDeclaration queueDeclaration = QueueDeclaration.create(queueName);

    final Integer bufferSize = 10;
    final Source<ReadResult, NotUsed> amqpSource =
        AmqpSource.atMostOnceSource(
            NamedQueueSourceSettings.create(connectionProvider, queueName)
                .withDeclaration(queueDeclaration),
            bufferSize);

    final List<String> input = Arrays.asList("one", "two", "three", "four", "five");

    // #create-rpc-flow
    final Flow<ByteString, ByteString, CompletionStage<String>> ampqRpcFlow =
        AmqpRpcFlow.createSimple(
            AmqpWriteSettings.create(connectionProvider)
                .withRoutingKey(queueName)
                .withDeclaration(queueDeclaration),
            1);

    Pair<CompletionStage<String>, TestSubscriber.Probe<ByteString>> result =
        Source.from(input)
            .map(ByteString::fromString)
            .viaMat(ampqRpcFlow, Keep.right())
            .toMat(TestSink.probe(system), Keep.both())
            .run(system);
    // #create-rpc-flow
    result.first().toCompletableFuture().get(3, TimeUnit.SECONDS);

    Sink<WriteMessage, CompletionStage<Done>> amqpSink =
        AmqpSink.createReplyTo(AmqpReplyToSinkSettings.create(connectionProvider));

    UniqueKillSwitch killSwitch =
        amqpSource
            .viaMat(KillSwitches.single(), Keep.right())
            .map(
                b ->
                    WriteMessage.create(b.bytes().concat(ByteString.fromString("a")))
                        .withProperties(b.properties()))
            .to(amqpSink)
            .run(system);

    result
        .second()
        .request(5)
        .expectNextUnordered(
            ByteString.fromString("onea"),
            ByteString.fromString("twoa"),
            ByteString.fromString("threea"),
            ByteString.fromString("foura"),
            ByteString.fromString("fivea"))
        .expectComplete();

    killSwitch.shutdown();
  }

  @Test
  public void publishFanoutAndConsume() throws Exception {
    // #exchange-declaration
    final String exchangeName = "amqp-conn-it-test-pub-sub-" + System.currentTimeMillis();
    final ExchangeDeclaration exchangeDeclaration =
        ExchangeDeclaration.create(exchangeName, "fanout");
    // #exchange-declaration

    // #create-exchange-sink
    final Sink<ByteString, CompletionStage<Done>> amqpSink =
        AmqpSink.createSimple(
            AmqpWriteSettings.create(connectionProvider)
                .withExchange(exchangeName)
                .withDeclaration(exchangeDeclaration));
    // #create-exchange-sink

    // #create-exchange-source
    final int fanoutSize = 4;
    final int bufferSize = 1;

    Source<Pair<Integer, String>, NotUsed> mergedSources = Source.empty();
    for (int i = 0; i < fanoutSize; i++) {
      final int fanoutBranch = i;
      mergedSources =
          mergedSources.merge(
              AmqpSource.atMostOnceSource(
                      TemporaryQueueSourceSettings.create(connectionProvider, exchangeName)
                          .withDeclaration(exchangeDeclaration),
                      bufferSize)
                  .map(msg -> Pair.create(fanoutBranch, msg.bytes().utf8String())));
    }
    // #create-exchange-source

    final CompletableFuture<Done> completion = new CompletableFuture<>();
    UniqueKillSwitch mergingFlow =
        mergedSources
            .viaMat(KillSwitches.single(), Keep.right())
            .to(
                Sink.fold(
                    new HashSet<Integer>(),
                    (seen, branchElem) -> {
                      if (seen.size() == fanoutSize) {
                        completion.complete(Done.getInstance());
                      }
                      seen.add(branchElem.first());
                      return seen;
                    }))
            .run(system);

    system
        .scheduler()
        .scheduleOnce(
            Duration.ofSeconds(5),
            () ->
                completion.completeExceptionally(
                    new Error("Did not get at least one element from every fanout branch")),
            system.dispatcher());

    UniqueKillSwitch repeatingFlow =
        Source.repeat("stuff")
            .viaMat(KillSwitches.single(), Keep.right())
            .map(ByteString::fromString)
            .to(amqpSink)
            .run(system);

    assertEquals(Done.getInstance(), completion.get(10, TimeUnit.SECONDS));
    mergingFlow.shutdown();
    repeatingFlow.shutdown();
  }

  private CompletionStage<CommittableReadResult> businessLogic(CommittableReadResult msg) {
    return CompletableFuture.completedFuture(msg);
  }

  @Test
  public void publishAndConsumeWithoutAutoAck() throws Exception {
    final String queueName = "amqp-conn-it-test-no-auto-ack-" + System.currentTimeMillis();
    final QueueDeclaration queueDeclaration = QueueDeclaration.create(queueName);

    final Sink<ByteString, CompletionStage<Done>> amqpSink =
        AmqpSink.createSimple(
            AmqpWriteSettings.create(connectionProvider)
                .withRoutingKey(queueName)
                .withDeclaration(queueDeclaration));

    final List<String> input = Arrays.asList("one", "two", "three", "four", "five");
    Source.from(input).map(ByteString::fromString).runWith(amqpSink, system);

    // #create-source-withoutautoack
    final Integer bufferSize = 10;
    final Source<CommittableReadResult, NotUsed> amqpSource =
        AmqpSource.committableSource(
            NamedQueueSourceSettings.create(connectionProvider, queueName)
                .withDeclaration(queueDeclaration),
            bufferSize);

    final CompletionStage<List<ReadResult>> result =
        amqpSource
            .mapAsync(1, this::businessLogic)
            .mapAsync(1, cm -> cm.ack(/* multiple */ false).thenApply(unused -> cm.message()))
            .take(input.size())
            .runWith(Sink.seq(), system);
    // #create-source-withoutautoack

    assertEquals(
        input,
        result.toCompletableFuture().get(3, TimeUnit.SECONDS).stream()
            .map(m -> m.bytes().utf8String())
            .collect(Collectors.toList()));
  }

  @Test
  public void republishMessageWithoutAutoAckIfNacked() throws Exception {
    final String queueName = "amqp-conn-it-test-no-auto-ack-nacked-" + System.currentTimeMillis();
    final QueueDeclaration queueDeclaration = QueueDeclaration.create(queueName);

    final Sink<ByteString, CompletionStage<Done>> amqpSink =
        AmqpSink.createSimple(
            AmqpWriteSettings.create(connectionProvider)
                .withRoutingKey(queueName)
                .withDeclaration(queueDeclaration));

    final List<String> input = Arrays.asList("one", "two", "three", "four", "five");
    Source.from(input)
        .map(ByteString::fromString)
        .runWith(amqpSink, system)
        .toCompletableFuture()
        .get(5, TimeUnit.SECONDS);

    final Integer bufferSize = 10;
    final Source<CommittableReadResult, NotUsed> amqpSource =
        AmqpSource.committableSource(
            NamedQueueSourceSettings.create(connectionProvider, queueName)
                .withDeclaration(queueDeclaration),
            bufferSize);

    // #create-source-withoutautoack

    final CompletionStage<List<ReadResult>> nackedResults =
        amqpSource
            .take(input.size())
            .mapAsync(1, this::businessLogic)
            .mapAsync(
                1,
                cm ->
                    cm.nack(/* multiple */ false, /* requeue */ true)
                        .thenApply(unused -> cm.message()))
            .runWith(Sink.seq(), system);
    // #create-source-withoutautoack

    nackedResults.toCompletableFuture().get(3, TimeUnit.SECONDS);

    final CompletionStage<List<CommittableReadResult>> result2 =
        amqpSource
            .take(input.size())
            .mapAsync(1, cm -> cm.ack().thenApply(unused -> cm))
            .runWith(Sink.seq(), system);

    assertEquals(
        input,
        result2.toCompletableFuture().get(10, TimeUnit.SECONDS).stream()
            .map(m -> m.message().bytes().utf8String())
            .collect(Collectors.toList()));

    // See https://github.com/akka/akka/issues/26410
    // extra wait before assertAllStagesStopped kicks in
    Thread.sleep(6 * 1000);
  }

  @Test
  public void shouldPublishWithFlow() throws Exception {
    final String queueName = "amqp-conn-it-test-flow-" + System.currentTimeMillis();
    final QueueDeclaration queueDeclaration = QueueDeclaration.create(queueName);

    // #create-flow
    final AmqpWriteSettings settings =
        AmqpWriteSettings.create(connectionProvider)
            .withRoutingKey(queueName)
            .withDeclaration(queueDeclaration)
            .withBufferSize(10)
            .withConfirmationTimeout(Duration.ofMillis(200));

    final Flow<WriteMessage, WriteResult, CompletionStage<Done>> amqpFlow =
        AmqpFlow.createWithConfirm(settings);

    final List<String> input = Arrays.asList("one", "two", "three", "four", "five");

    final List<WriteResult> result =
        Source.from(input)
            .map(message -> WriteMessage.create(ByteString.fromString(message)))
            .via(amqpFlow)
            .runWith(Sink.seq(), system)
            .toCompletableFuture()
            .get();
    // #create-flow

    final List<WriteResult> expectedResult =
        input.stream().map(s -> WriteResult.create(true)).collect(Collectors.toList());

    assertEquals(result, expectedResult);
  }
}
