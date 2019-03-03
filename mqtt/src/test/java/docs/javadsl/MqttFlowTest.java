/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.mqtt.*;
import akka.stream.alpakka.mqtt.javadsl.MqttFlow;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.junit.Assert.assertFalse;

public class MqttFlowTest {

  private static ActorSystem system;
  private static Materializer materializer;

  private static final int bufferSize = 8;

  private static Pair<ActorSystem, Materializer> setupMaterializer() {
    final ActorSystem system = ActorSystem.create("MqttFlowTest");
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
  public void establishBidirectionalConnectionAndSubscribeToATopic() throws Exception {
    final MqttConnectionSettings connectionSettings =
        MqttConnectionSettings.create(
            "tcp://localhost:1883", "test-java-client", new MemoryPersistence());

    // #create-flow
    final Flow<MqttMessage, MqttMessage, CompletionStage<Done>> mqttFlow =
        MqttFlow.atMostOnce(
            connectionSettings,
            MqttSubscriptions.create("flow-test/topic", MqttQoS.atMostOnce()),
            bufferSize,
            MqttQoS.atLeastOnce());
    // #create-flow

    final Source<MqttMessage, CompletableFuture<Optional<MqttMessage>>> source = Source.maybe();

    // #run-flow
    final Pair<
            Pair<CompletableFuture<Optional<MqttMessage>>, CompletionStage<Done>>,
            CompletionStage<List<MqttMessage>>>
        materialized =
            source.viaMat(mqttFlow, Keep.both()).toMat(Sink.seq(), Keep.both()).run(materializer);

    CompletableFuture<Optional<MqttMessage>> mqttMessagePromise = materialized.first().first();
    CompletionStage<Done> subscribedToMqtt = materialized.first().second();
    CompletionStage<List<MqttMessage>> streamResult = materialized.second();
    // #run-flow

    subscribedToMqtt.thenAccept(
        a -> {
          mqttMessagePromise.complete(Optional.empty());
          assertFalse(streamResult.toCompletableFuture().isCompletedExceptionally());
        });
  }
}
