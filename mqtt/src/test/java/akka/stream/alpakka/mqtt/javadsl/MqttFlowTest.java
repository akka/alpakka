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

import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.util.*;
import java.util.concurrent.*;

public class MqttFlowTest {

  static ActorSystem system;
  static Materializer materializer;

  public static Pair<ActorSystem, Materializer> setupMaterializer() {
    final ActorSystem system = ActorSystem.create();
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
        JavaTestKit.shutdownActorSystem(system);
    }

  @Test
  public void establishBidirectionalConnectionAndSubscribeToATopic() throws Exception {
    final MqttConnectionSettings connectionSettings = MqttConnectionSettings.create(
      "tcp://localhost:1883",
      "test-java-client",
      new MemoryPersistence()
    );

    final MqttSourceSettings settings = MqttSourceSettings
      .create(connectionSettings.withClientId("flow-test/flow"))
      .withSubscriptions(Pair.create("flow-test/topic", MqttQoS.atMostOnce()));

    //#create-flow
    final Flow<MqttMessage, MqttMessage, CompletionStage<Done>> mqttFlow =
      MqttFlow.create(settings, 8, MqttQoS.atLeastOnce());
    //#create-flow

    final Source<MqttMessage, CompletableFuture<Optional<MqttMessage>>> source = Source.maybe();

    //#run-flow
    final Pair<Pair<CompletableFuture<Optional<MqttMessage>>, CompletionStage<Done>>, CompletionStage<List<MqttMessage>>> result =
      source
        .viaMat(mqttFlow, Keep.both())
        .toMat(Sink.seq(), Keep.both())
        .run(materializer);
    //#run-flow

    result.first().second().thenAccept(a -> {
      result.first().first().complete(Optional.empty());
      assertFalse(result.second().toCompletableFuture().isCompletedExceptionally());
    });
  }
}
