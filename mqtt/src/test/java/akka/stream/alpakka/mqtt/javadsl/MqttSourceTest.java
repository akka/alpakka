/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.javadsl;

import akka.Done;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.KillSwitches;
import akka.stream.Materializer;
import akka.stream.UniqueKillSwitch;
import akka.stream.alpakka.mqtt.MqttConnectionSettings;
import akka.stream.alpakka.mqtt.MqttMessage;
import akka.stream.alpakka.mqtt.MqttQoS;
import akka.stream.alpakka.mqtt.MqttSourceSettings;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.Tcp;
import akka.stream.testkit.TestSubscriber;
import akka.stream.testkit.javadsl.TestSink;
import akka.testkit.javadsl.TestKit;
import akka.util.ByteString;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class MqttSourceTest {

  private static ActorSystem system;
  private static Materializer materializer;

  private static Pair<ActorSystem, Materializer> setupMaterializer() {
    //#init-mat
    final ActorSystem system = ActorSystem.create();
    final Materializer materializer = ActorMaterializer.create(system);
    //#init-mat
    return Pair.create(system, materializer);
  }

  @BeforeClass
  public static void setup() throws Exception {
    final Pair<ActorSystem, Materializer> sysmat = setupMaterializer();
    system = sysmat.first();
    materializer = sysmat.second();
  }

  @AfterClass
  public static void teardown() {
    TestKit.shutdownActorSystem(system);
  }

  @Test
  public void publishAndConsumeWithoutAutoAck() throws Exception {
    final String topic = "source-test/manualacks";
    final MqttConnectionSettings baseConnectionSettings = MqttConnectionSettings.create(
      "tcp://localhost:1883",
      "test-java-client",
      new MemoryPersistence()
    );

    MqttConnectionSettings sourceSettings = baseConnectionSettings.withClientId("source-test/source-withoutAutoAck");
    MqttConnectionSettings sinkSettings = baseConnectionSettings.withClientId("source-test/sink-withoutAutoAck");

    final List<String> input = Arrays.asList("one", "two", "three", "four", "five");

    //#create-source-with-manualacks
    MqttConnectionSettings connectionSettings = sourceSettings.withCleanSession(false);
    @SuppressWarnings("unchecked")
    MqttSourceSettings mqttSourceSettings = MqttSourceSettings.create(connectionSettings)
      .withSubscriptions(Pair.create(topic, MqttQoS.atLeastOnce()));
    final Source<MqttCommittableMessage, CompletionStage<Done>> mqttSource = MqttSource.atLeastOnce(mqttSourceSettings, 8);
    //#create-source-with-manualacks

    final Pair<CompletionStage<Done>, CompletionStage<List<MqttCommittableMessage>>> unackedResult = mqttSource
      .take(input.size())
      .toMat(Sink.seq(), Keep.both())
      .run(materializer);

    unackedResult.first().toCompletableFuture().get(5, TimeUnit.SECONDS);

    final Sink<MqttMessage, CompletionStage<Done>> mqttSink = MqttSink.create(sinkSettings, MqttQoS.atLeastOnce());
    Source.from(input).map(s -> MqttMessage.create(topic, ByteString.fromString(s))).runWith(mqttSink, materializer);

    assertEquals(input, unackedResult.second().toCompletableFuture().get(5, TimeUnit.SECONDS).stream().map(m -> m.message().payload().utf8String()).collect(Collectors.toList()));

    //#run-source-with-manualacks
    final CompletionStage<List<MqttMessage>> result = mqttSource
      .mapAsync(1, cm -> cm.messageArrivedComplete().thenApply(unused2 -> cm.message()))
      .take(input.size())
      .runWith(Sink.seq(), materializer);
    //#run-source-with-manualacks

    assertEquals(input, result.toCompletableFuture().get(3, TimeUnit.SECONDS).stream().map(m -> m.payload().utf8String()).collect(Collectors.toList()));
  }

  @Test
  public void keepConnectionOpenIfDownstreamClosesAndThereArePendingAcks() throws Exception {
    final String topic = "source-test/pendingacks";
    final MqttConnectionSettings baseConnectionSettings = MqttConnectionSettings.create(
            "tcp://localhost:1883",
            "test-java-client",
            new MemoryPersistence()
    );

    MqttConnectionSettings sourceSettings = baseConnectionSettings.withClientId("source-test/source-pending");
    MqttConnectionSettings sinkSettings = baseConnectionSettings.withClientId("source-test/sink-pending");

    final Sink<MqttMessage, CompletionStage<Done>> mqttSink = MqttSink.create(sinkSettings, MqttQoS.atLeastOnce());
    final List<String> input = Arrays.asList("one", "two", "three", "four", "five");

    MqttConnectionSettings connectionSettings = sourceSettings.withCleanSession(false);
    @SuppressWarnings("unchecked")
    MqttSourceSettings mqttSourceSettings = MqttSourceSettings.create(connectionSettings)
            .withSubscriptions(Pair.create(topic, MqttQoS.atLeastOnce()));
    final Source<MqttCommittableMessage, CompletionStage<Done>> mqttSource = MqttSource.atLeastOnce(mqttSourceSettings, 8);

    final Pair<CompletionStage<Done>, CompletionStage<List<MqttCommittableMessage>>> unackedResult = mqttSource
            .take(input.size())
            .toMat(Sink.seq(), Keep.both())
            .run(materializer);

    unackedResult.first().toCompletableFuture().get(5, TimeUnit.SECONDS);

    Source.from(input).map(s -> MqttMessage.create(topic, ByteString.fromString(s)))
          .runWith(mqttSink, materializer)
          .toCompletableFuture()
          .get(3, TimeUnit.SECONDS);

    unackedResult.second().toCompletableFuture().get(5, TimeUnit.SECONDS)
      .forEach(m -> {
        try {
        m.messageArrivedComplete().toCompletableFuture().get(3, TimeUnit.SECONDS);
        } catch (Exception e) {
        assertEquals("Error acking message manually", false, true);
        }
      });
  }

  @Test
  public void receiveFromMultipleTopics() throws Exception {
    final String topic1 = "source-test/topic1";
    final String topic2 = "source-test/topic2";

    //#create-connection-settings
    final MqttConnectionSettings connectionSettings = MqttConnectionSettings.create(
      "tcp://localhost:1883",
      "test-java-client",
      new MemoryPersistence()
    );
    //#create-connection-settings

    final Integer messageCount = 7;

    @SuppressWarnings("unchecked")
    //#create-source
    final MqttSourceSettings settings = MqttSourceSettings
      .create(connectionSettings.withClientId("source-test/source"))
      .withSubscriptions(
        Pair.create(topic1, MqttQoS.atMostOnce()),
        Pair.create(topic2, MqttQoS.atMostOnce())
      );

    final Integer bufferSize = 8;
    final Source<MqttMessage, CompletionStage<Done>> mqttSource = MqttSource.create(settings, bufferSize);
    //#create-source

    //#run-source
    final Pair<CompletionStage<Done>, CompletionStage<List<String>>> result = mqttSource
      .map(m -> m.topic() + "-" + m.payload().utf8String())
      .take(messageCount * 2)
      .toMat(Sink.seq(), Keep.both())
      .run(materializer);
    //#run-source

    result.first().toCompletableFuture().get(3, TimeUnit.SECONDS);

    List<MqttMessage> messages = IntStream.range(0, messageCount).boxed()
      .flatMap(i -> Stream.of(
        MqttMessage.create(topic1, ByteString.fromString("msg" + i.toString())),
        MqttMessage.create(topic2, ByteString.fromString("msg" + i.toString()))))
      .collect(Collectors.toList());

    //#run-sink
    Sink<MqttMessage, CompletionStage<Done>> mqttSink = MqttSink.create(
      connectionSettings.withClientId("source-test/sink"),
      MqttQoS.atLeastOnce()
    );
    Source.from(messages).runWith(mqttSink, materializer);
    //#run-sink

    assertEquals(
      IntStream.range(0, messageCount).boxed()
        .flatMap(i -> Stream.of("source-test/topic1-msg" + i, "source-test/topic2-msg" + i))
        .collect(Collectors.toSet()),
        new HashSet<>(result.second().toCompletableFuture().get(3, TimeUnit.SECONDS))
    );
  }

  @Test
  public void supportWillMessage() throws Exception {
    String topic1 = "source-spec/topic1";
    String willTopic = "source-spec/will";
    final MqttConnectionSettings baseConnectionSettings = MqttConnectionSettings.create(
            "tcp://localhost:1883",
            "test-java-client",
            new MemoryPersistence()
    );
    MqttConnectionSettings sourceSettings = baseConnectionSettings.withClientId("source-test/source-withoutAutoAck");
    MqttConnectionSettings sinkSettings = baseConnectionSettings.withClientId("source-test/sink-withoutAutoAck");

    MqttMessage msg = MqttMessage.create(topic1, ByteString.fromString("ohi"));

    //#will-message
    MqttMessage lastWill = MqttMessage.create(
            willTopic,
            ByteString.fromString("ohi"),
            MqttQoS.atLeastOnce(),
            true);
    //#will-message

    // Create a proxy to RabbitMQ so it can be shutdown
    Pair<CompletionStage<Tcp.ServerBinding>, CompletionStage<Tcp.IncomingConnection>> result1 = Tcp.get(system)
            .bind("localhost", 1337).toMat(Sink.head(), Keep.both()).run(materializer);

    CompletionStage<UniqueKillSwitch> proxyKs = result1.second().toCompletableFuture().thenApply(conn ->
      conn.handleWith(
        Tcp.get(system)
          .outgoingConnection("localhost", 1883)
          .viaMat(KillSwitches.single(), Keep.right()),
        materializer
      )
    );

    result1.first().toCompletableFuture().get(5, TimeUnit.SECONDS);

    @SuppressWarnings("unchecked")
    MqttSourceSettings settings1 = MqttSourceSettings.create(
      sourceSettings
        .withClientId("source-spec/testator")
        .withBroker("tcp://localhost:1337")
        .withWill(lastWill)
    ).withSubscriptions(Pair.create(topic1, MqttQoS.atLeastOnce()));

    Source<MqttMessage, CompletionStage<Done>> source1 = MqttSource.atMostOnce(settings1, 8);

    Pair<CompletionStage<Done>, TestSubscriber.Probe<MqttMessage>> result2 = source1.toMat(TestSink.probe(system), Keep.both()).run(materializer);

    // Ensure that the connection made it all the way to the server by waiting until it receives a message
    result2.first().toCompletableFuture().get(5, TimeUnit.SECONDS);
    Source.single(msg).runWith(MqttSink.create(sinkSettings, MqttQoS.atLeastOnce()), materializer);
    result2.second().requestNext();

    // Kill the proxy, producing an unexpected disconnection of the client
    proxyKs.toCompletableFuture().get(5, TimeUnit.SECONDS).shutdown();

    @SuppressWarnings("unchecked")
    MqttSourceSettings settings2 = MqttSourceSettings
        .create(sourceSettings.withClientId("source-spec/executor"))
        .withSubscriptions(Pair.create(willTopic, MqttQoS.atLeastOnce()));
    Source<MqttMessage, CompletionStage<Done>> source2 = MqttSource.atMostOnce(settings2, 8);

    CompletionStage<MqttMessage> elem = source2.runWith(Sink.head(), materializer);
    assertEquals(
      MqttMessage.create(willTopic, ByteString.fromString("ohi")),
      elem.toCompletableFuture().get(3, TimeUnit.SECONDS)
    );
  }
}
