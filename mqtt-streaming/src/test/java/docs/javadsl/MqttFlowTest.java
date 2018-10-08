/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.OverflowStrategy;
import akka.stream.alpakka.mqtt.streaming.Command;
import akka.stream.alpakka.mqtt.streaming.Connect;
import akka.stream.alpakka.mqtt.streaming.ConnectFlags;
import akka.stream.alpakka.mqtt.streaming.DecodeErrorOrEvent;
import akka.stream.alpakka.mqtt.streaming.MqttSessionSettings;
import akka.stream.alpakka.mqtt.streaming.Publish;
import akka.stream.alpakka.mqtt.streaming.Subscribe;
import akka.stream.alpakka.mqtt.streaming.javadsl.ActorMqttClientSession;
import akka.stream.alpakka.mqtt.streaming.javadsl.Mqtt;
import akka.stream.alpakka.mqtt.streaming.javadsl.MqttClientSession;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.SourceQueueWithComplete;
import akka.stream.javadsl.Tcp;
import akka.testkit.javadsl.TestKit;
import akka.util.ByteString;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertTrue;

public class MqttFlowTest {

  private static ActorSystem system;
  private static Materializer materializer;

  private static Pair<ActorSystem, Materializer> setupMaterializer() {
    final ActorSystem system = ActorSystem.create("MqttFlowTest");
    final Materializer materializer = ActorMaterializer.create(system);
    return Pair.create(system, materializer);
  }

  @BeforeClass
  public static void setup() {
    final Pair<ActorSystem, Materializer> sysmat = setupMaterializer();
    system = sysmat.first();
    materializer = sysmat.second();
  }

  @AfterClass
  public static void teardown() {
    TestKit.shutdownActorSystem(system);
  }

  @Test
  public void establishBidirectionalConnectionAndSubscribeToATopic()
      throws InterruptedException, ExecutionException, TimeoutException {
    String clientId = "flow-spec/flow";
    String topic = "source-spec/topic1";

    // #create-streaming-flow
    MqttSessionSettings settings = MqttSessionSettings.create();
    MqttClientSession session = new ActorMqttClientSession(settings, system);

    Flow<ByteString, ByteString, CompletionStage<Tcp.OutgoingConnection>> connection =
            Tcp.get(system).outgoingConnection("localhost", 1883);

    Flow<Command<?>, DecodeErrorOrEvent, NotUsed> mqttFlow =
        Mqtt.clientSessionFlow(session).join(connection);
    // #create-streaming-flow

    // #run-streaming-flow
    Pair<SourceQueueWithComplete<Command<?>>, CompletionStage<DecodeErrorOrEvent>> run =
        Source.<Command<?>>queue(3, OverflowStrategy.fail())
            .via(mqttFlow)
            .drop(3)
            .toMat(Sink.head(), Keep.both())
            .run(materializer);

    SourceQueueWithComplete<Command<?>> commands = run.first();
    commands.offer(new Command(new Connect(clientId, ConnectFlags.None())));
    commands.offer(new Command(new Subscribe(topic)));
    commands.offer(new Command(new Publish(topic, ByteString.fromString("ohi"))));
    // #run-streaming-flow

    CompletionStage<DecodeErrorOrEvent> event = run.second();
    DecodeErrorOrEvent decodeErrorOrEvent = event.toCompletableFuture().get(3, TimeUnit.SECONDS);
    assertTrue(decodeErrorOrEvent.getEvent().isPresent());
  }
}
