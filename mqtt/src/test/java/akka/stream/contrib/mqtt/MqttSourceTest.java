package akka.stream.contrib.mqtt;

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

import io.moquette.proto.messages.AbstractMessage.QOSType;
import io.moquette.proto.messages.PublishMessage;
import io.moquette.server.Server;

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

    server = new Server();
    server.startServer();
  }

  @AfterClass
  public static void teardown() {
    server.stopServer();
    JavaTestKit.shutdownActorSystem(system);
  }

  public void publish(String topic, String payload) {
    final PublishMessage  msg = new PublishMessage();
    msg.setPayload(ByteString.fromString(payload).toByteBuffer());
    msg.setTopicName(topic);
    msg.setQos(QOSType.valueOf(MqttQoS.atMostOnce().byteValue()));
    server.internalPublish(msg);
  }

  @Test
  public void receiveFromMultipleTopics() throws Exception {
    //#create-settings
    final MqttSourceSettings settings = MqttSourceSettings.create(
      MqttConnectionSettings.create(
        "tcp://localhost:1883",
        "test-client",
        new MemoryPersistence()
      )
    ).withSubscriptions(
      Pair.create("topic1", MqttQoS.atMostOnce()),
      Pair.create("topic2", MqttQoS.atMostOnce())
    );
    //#create-settings

    final Integer messageCount = 7;

    //#create-source
    final Integer bufferSize = 8;
    final Source<MqttMessage, CompletionStage<Done>> mqttSource =
      MqttSource.create(settings, bufferSize);
    //#create-source

    //#run-source
    final Pair<CompletionStage<Done>, CompletionStage<List<String>>> result = mqttSource
      .map(m -> m.topic() + "_" + m.payload().utf8String())
      .take(messageCount * 2)
      .toMat(Sink.seq(), Keep.both())
      .run(materializer);
    //#run-source

    result.first().toCompletableFuture().get(3, TimeUnit.SECONDS);
    for (Integer i = 0; i < messageCount; i++) {
      publish("topic1", "msg" + i);
      publish("topic2", "msg" + i);
    }

    assertEquals(
      IntStream.range(0, messageCount).boxed()
        .flatMap(i -> Arrays.asList("topic1_msg" + i, "topic2_msg" + i).stream())
        .collect(Collectors.toSet()),
      result.second().toCompletableFuture().get(3, TimeUnit.SECONDS).stream().collect(Collectors.toSet()));
  }

}
