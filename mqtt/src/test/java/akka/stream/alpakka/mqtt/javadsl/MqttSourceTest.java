/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.javadsl;

import akka.stream.alpakka.mqtt.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

import akka.Done;
import akka.actor.*;
import akka.stream.*;
import akka.stream.javadsl.*;
import akka.testkit.*;
import akka.japi.Pair;
import akka.util.ByteString;

import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

public class MqttSourceTest {

  static ActorSystem system;
  static Materializer materializer;

  static Server server;

  public static Pair<ActorSystem, Materializer> setupMaterializer() {
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
    JavaTestKit.shutdownActorSystem(system);
  }

  @Test
  public void publishAndConsumeWithoutAutoAck() throws Exception {
    final String topic = "source-test/topic1";
    final MqttConnectionSettings baseConnectionSettings = MqttConnectionSettings.create(
            "tcp://localhost:1883",
            "test-java-client",
            new MemoryPersistence()
    );

    MqttConnectionSettings sourceSettings = baseConnectionSettings.withClientId("source-spec/source");
    MqttConnectionSettings sinkSettings = baseConnectionSettings.withClientId("source-spec/sink");

    final Sink<MqttMessage, CompletionStage<Done>> mqttSink = MqttSink.create(sinkSettings, MqttQoS.atLeastOnce());
    final List<String> input = Arrays.asList("one", "two", "three", "four", "five");
    Source.from(input).map(s -> new MqttMessage(topic, ByteString.fromString(s))).runWith(mqttSink, materializer);

    //#create-source-with-manualacks
    MqttConnectionSettings connectionSettings = sourceSettings.withCleanSession(false);
    MqttSourceSettings mqttSourceSettings = MqttSourceSettings.create(connectionSettings)
            .withSubscriptions(Pair.create(topic, MqttQoS.atLeastOnce()));
    final Source<MqttCommittableMessage, CompletionStage<Done>> mqttSource = MqttSource.atLeastOnce(mqttSourceSettings, 10);
    //#create-source-with-manualacks

    final CompletionStage<List<MqttCommittableMessage>> unackedResult = mqttSource
            .take(input.size())
            .runWith(Sink.seq(), materializer);
    assertEquals(input, unackedResult.toCompletableFuture().get(3, TimeUnit.SECONDS).stream().map(m -> m.message().payload().utf8String()).collect(Collectors.toList()));

    //#run-source-with-manualacks
    final CompletionStage<List<MqttMessage>> result =
            mqttSource
                    .mapAsync(1, cm -> cm.messageArrivedComplete().thenApply(unused -> cm.message()))
                    .take(input.size())
                    .runWith(Sink.seq(), materializer);
    //#run-source-with-manualacks

    assertEquals(input, result.toCompletableFuture().get(3, TimeUnit.SECONDS).stream().map(m -> m.payload().utf8String()).collect(Collectors.toList()));
  }

  @Test
  public void receiveFromMultipleTopics() throws Exception {
    //#create-connection-settings
    final MqttConnectionSettings connectionSettings = MqttConnectionSettings.create(
      "tcp://localhost:1883",
      "test-java-client",
      new MemoryPersistence()
    );
    //#create-connection-settings

    final Integer messageCount = 7;

    //#create-source
    final MqttSourceSettings settings = MqttSourceSettings.create(
      connectionSettings.withClientId("source-test/source")
    ).withSubscriptions(
      Pair.create("source-test/topic1", MqttQoS.atMostOnce()),
      Pair.create("source-test/topic2", MqttQoS.atMostOnce())
    );

    final Integer bufferSize = 8;
    final Source<MqttMessage, CompletionStage<Done>> mqttSource =
      MqttSource.create(settings, bufferSize);
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
      .flatMap(i -> Arrays.asList(
        MqttMessage.create("source-test/topic1", ByteString.fromString("msg" + i.toString())),
        MqttMessage.create("source-test/topic2", ByteString.fromString("msg" + i.toString()))).stream())
      .collect(Collectors.toList());

    //#run-sink
    Source.from(messages).runWith(MqttSink.create(
      connectionSettings.withClientId("source-test/sink"),
      MqttQoS.atLeastOnce()), materializer);
    //#run-sink

    assertEquals(
      IntStream.range(0, messageCount).boxed()
        .flatMap(i -> Arrays.asList("source-test/topic1-msg" + i, "source-test/topic2-msg" + i).stream())
        .collect(Collectors.toSet()),
      result.second().toCompletableFuture().get(3, TimeUnit.SECONDS).stream().collect(Collectors.toSet()));
  }
}
