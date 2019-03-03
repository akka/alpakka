/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.KillSwitches;
import akka.stream.Materializer;
import akka.stream.UniqueKillSwitch;
import akka.stream.alpakka.mqtt.*;
import akka.stream.alpakka.mqtt.javadsl.MqttMessageWithAck;
import akka.stream.alpakka.mqtt.javadsl.MqttSink;
import akka.stream.alpakka.mqtt.javadsl.MqttSource;
import akka.stream.javadsl.*;
import akka.stream.testkit.TestSubscriber;
import akka.stream.testkit.javadsl.TestSink;
import akka.testkit.javadsl.TestKit;
import akka.util.ByteString;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.net.ssl.SSLContext;

import static org.hamcrest.CoreMatchers.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class MqttSourceTest {

  private static ActorSystem system;
  private static Materializer materializer;

  private static final int bufferSize = 8;

  private static Pair<ActorSystem, Materializer> setupMaterializer() {
    final ActorSystem system = ActorSystem.create("MqttSourceTest");
    final Materializer materializer = ActorMaterializer.create(system);
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
  public void connectionSettings() {
    // #create-connection-settings
    MqttConnectionSettings connectionSettings =
        MqttConnectionSettings.create(
            "tcp://localhost:1883", // (1)
            "test-java-client", // (2)
            new MemoryPersistence() // (3)
            );
    // #create-connection-settings
    assertThat(connectionSettings.toString(), containsString("tcp://localhost:1883"));
  }

  @Test
  public void connectionSettingsForSsl() throws Exception {
    // #ssl-settings
    MqttConnectionSettings connectionSettings =
        MqttConnectionSettings.create("ssl://localhost:1885", "ssl-client", new MemoryPersistence())
            .withAuth("mqttUser", "mqttPassword")
            .withSocketFactory(SSLContext.getDefault().getSocketFactory());
    // #ssl-settings
    assertThat(connectionSettings.toString(), containsString("ssl://localhost:1885"));
    assertThat(connectionSettings.toString(), containsString("auth(username)=Some(mqttUser)"));
  }

  @Test
  public void publishAndConsumeWithoutAutoAck() throws Exception {
    final String topic = "source-test/manualacks";
    final MqttConnectionSettings baseConnectionSettings =
        MqttConnectionSettings.create(
            "tcp://localhost:1883", "test-java-client", new MemoryPersistence());

    MqttConnectionSettings connectionSettings = baseConnectionSettings;

    final List<String> input = Arrays.asList("one", "two", "three", "four", "five");

    // #create-source-with-manualacks
    Source<MqttMessageWithAck, CompletionStage<Done>> mqttSource =
        MqttSource.atLeastOnce(
            connectionSettings
                .withClientId("source-test/source-withoutAutoAck")
                .withCleanSession(false),
            MqttSubscriptions.create(topic, MqttQoS.atLeastOnce()),
            bufferSize);
    // #create-source-with-manualacks

    final Pair<CompletionStage<Done>, CompletionStage<List<MqttMessageWithAck>>> unackedResult =
        mqttSource.take(input.size()).toMat(Sink.seq(), Keep.both()).run(materializer);

    unackedResult.first().toCompletableFuture().get(5, TimeUnit.SECONDS);

    final Sink<MqttMessage, CompletionStage<Done>> mqttSink =
        MqttSink.create(
            baseConnectionSettings.withClientId("source-test/sink-withoutAutoAck"),
            MqttQoS.atLeastOnce());
    Source.from(input)
        .map(s -> MqttMessage.create(topic, ByteString.fromString(s)))
        .runWith(mqttSink, materializer);

    assertEquals(
        input,
        unackedResult
            .second()
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS)
            .stream()
            .map(m -> m.message().payload().utf8String())
            .collect(Collectors.toList()));

    Flow<MqttMessageWithAck, MqttMessageWithAck, NotUsed> businessLogic = Flow.create();

    // #run-source-with-manualacks
    final CompletionStage<List<MqttMessage>> result =
        mqttSource
            .via(businessLogic)
            .mapAsync(
                1,
                messageWithAck ->
                    messageWithAck.ack().thenApply(unused2 -> messageWithAck.message()))
            .take(input.size())
            .runWith(Sink.seq(), materializer);
    // #run-source-with-manualacks

    assertEquals(
        input,
        result
            .toCompletableFuture()
            .get(3, TimeUnit.SECONDS)
            .stream()
            .map(m -> m.payload().utf8String())
            .collect(Collectors.toList()));
  }

  @Test
  public void keepConnectionOpenIfDownstreamClosesAndThereArePendingAcks() throws Exception {
    final String topic = "source-test/pendingacks";
    final MqttConnectionSettings baseConnectionSettings =
        MqttConnectionSettings.create(
            "tcp://localhost:1883", "test-java-client", new MemoryPersistence());

    MqttConnectionSettings sourceSettings =
        baseConnectionSettings.withClientId("source-test/source-pending");
    MqttConnectionSettings sinkSettings =
        baseConnectionSettings.withClientId("source-test/sink-pending");

    final Sink<MqttMessage, CompletionStage<Done>> mqttSink =
        MqttSink.create(sinkSettings, MqttQoS.atLeastOnce());
    final List<String> input = Arrays.asList("one", "two", "three", "four", "five");

    MqttConnectionSettings connectionSettings = sourceSettings.withCleanSession(false);
    MqttSubscriptions subscriptions = MqttSubscriptions.create(topic, MqttQoS.atLeastOnce());
    final Source<MqttMessageWithAck, CompletionStage<Done>> mqttSource =
        MqttSource.atLeastOnce(connectionSettings, subscriptions, bufferSize);

    final Pair<CompletionStage<Done>, CompletionStage<List<MqttMessageWithAck>>> unackedResult =
        mqttSource.take(input.size()).toMat(Sink.seq(), Keep.both()).run(materializer);

    unackedResult.first().toCompletableFuture().get(5, TimeUnit.SECONDS);

    Source.from(input)
        .map(s -> MqttMessage.create(topic, ByteString.fromString(s)))
        .runWith(mqttSink, materializer)
        .toCompletableFuture()
        .get(3, TimeUnit.SECONDS);

    unackedResult
        .second()
        .toCompletableFuture()
        .get(5, TimeUnit.SECONDS)
        .forEach(
            m -> {
              try {
                m.ack().toCompletableFuture().get(3, TimeUnit.SECONDS);
              } catch (Exception e) {
                assertEquals("Error acking message manually", false, true);
              }
            });
  }

  @Test
  public void receiveFromMultipleTopics() throws Exception {
    final String topic1 = "source-test/topic1";
    final String topic2 = "source-test/topic2";

    MqttConnectionSettings connectionSettings =
        MqttConnectionSettings.create(
            "tcp://localhost:1883", "test-java-client", new MemoryPersistence());

    final Integer messageCount = 7;

    // #create-source
    MqttSubscriptions subscriptions =
        MqttSubscriptions.create(topic1, MqttQoS.atMostOnce())
            .addSubscription(topic2, MqttQoS.atMostOnce());

    Source<MqttMessage, CompletionStage<Done>> mqttSource =
        MqttSource.atMostOnce(
            connectionSettings.withClientId("source-test/source"), subscriptions, bufferSize);

    Pair<CompletionStage<Done>, CompletionStage<List<String>>> materialized =
        mqttSource
            .map(m -> m.topic() + "-" + m.payload().utf8String())
            .take(messageCount * 2)
            .toMat(Sink.seq(), Keep.both())
            .run(materializer);

    CompletionStage<Done> subscribed = materialized.first();
    CompletionStage<List<String>> streamResult = materialized.second();
    // #create-source

    subscribed.toCompletableFuture().get(3, TimeUnit.SECONDS);

    List<MqttMessage> messages =
        IntStream.range(0, messageCount)
            .boxed()
            .flatMap(
                i ->
                    Stream.of(
                        MqttMessage.create(topic1, ByteString.fromString("msg" + i.toString())),
                        MqttMessage.create(topic2, ByteString.fromString("msg" + i.toString()))))
            .collect(Collectors.toList());

    // #run-sink
    Sink<MqttMessage, CompletionStage<Done>> mqttSink =
        MqttSink.create(connectionSettings.withClientId("source-test/sink"), MqttQoS.atLeastOnce());
    Source.from(messages).runWith(mqttSink, materializer);
    // #run-sink

    assertEquals(
        IntStream.range(0, messageCount)
            .boxed()
            .flatMap(i -> Stream.of("source-test/topic1-msg" + i, "source-test/topic2-msg" + i))
            .collect(Collectors.toSet()),
        new HashSet<>(streamResult.toCompletableFuture().get(3, TimeUnit.SECONDS)));
  }

  @Test
  public void supportWillMessage() throws Exception {
    String topic1 = "source-test/topic1";
    String willTopic = "source-test/will";
    final MqttConnectionSettings baseConnectionSettings =
        MqttConnectionSettings.create(
            "tcp://localhost:1883", "test-java-client", new MemoryPersistence());
    MqttConnectionSettings sourceSettings =
        baseConnectionSettings.withClientId("source-test/source-withoutAutoAck");
    MqttConnectionSettings sinkSettings =
        baseConnectionSettings.withClientId("source-test/sink-withoutAutoAck");

    MqttMessage msg = MqttMessage.create(topic1, ByteString.fromString("ohi"));

    // #will-message
    MqttMessage lastWill =
        MqttMessage.create(willTopic, ByteString.fromString("ohi"))
            .withQos(MqttQoS.atLeastOnce())
            .withRetained(true);
    // #will-message

    // Create a proxy to RabbitMQ so it can be shutdown
    int proxyPort = 1347; // make sure to keep it separate from ports used by other tests
    Pair<CompletionStage<Tcp.ServerBinding>, CompletionStage<Tcp.IncomingConnection>> result1 =
        Tcp.get(system)
            .bind("localhost", proxyPort)
            .toMat(Sink.head(), Keep.both())
            .run(materializer);

    CompletionStage<UniqueKillSwitch> proxyKs =
        result1
            .second()
            .toCompletableFuture()
            .thenApply(
                conn ->
                    conn.handleWith(
                        Tcp.get(system)
                            .outgoingConnection("localhost", 1883)
                            .viaMat(KillSwitches.single(), Keep.right()),
                        materializer));

    result1.first().toCompletableFuture().get(5, TimeUnit.SECONDS);

    MqttConnectionSettings settings1 =
        sourceSettings
            .withClientId("source-test/testator")
            .withBroker("tcp://localhost:" + String.valueOf(proxyPort))
            .withWill(lastWill);
    MqttSubscriptions subscriptions = MqttSubscriptions.create(topic1, MqttQoS.atLeastOnce());

    Source<MqttMessage, CompletionStage<Done>> source1 =
        MqttSource.atMostOnce(settings1, subscriptions, bufferSize);

    Pair<CompletionStage<Done>, TestSubscriber.Probe<MqttMessage>> result2 =
        source1.toMat(TestSink.probe(system), Keep.both()).run(materializer);

    // Ensure that the connection made it all the way to the server by waiting until it receives a
    // message
    result2.first().toCompletableFuture().get(5, TimeUnit.SECONDS);
    Source.single(msg).runWith(MqttSink.create(sinkSettings, MqttQoS.atLeastOnce()), materializer);
    result2.second().requestNext();

    // Kill the proxy, producing an unexpected disconnection of the client
    proxyKs.toCompletableFuture().get(5, TimeUnit.SECONDS).shutdown();

    MqttConnectionSettings settings2 = sourceSettings.withClientId("source-test/executor");
    MqttSubscriptions subscriptions2 = MqttSubscriptions.create(willTopic, MqttQoS.atLeastOnce());
    Source<MqttMessage, CompletionStage<Done>> source2 =
        MqttSource.atMostOnce(settings2, subscriptions2, bufferSize);

    CompletionStage<MqttMessage> elem = source2.runWith(Sink.head(), materializer);
    assertEquals(
        MqttMessage.create(willTopic, ByteString.fromString("ohi")),
        elem.toCompletableFuture().get(3, TimeUnit.SECONDS));
  }
}
