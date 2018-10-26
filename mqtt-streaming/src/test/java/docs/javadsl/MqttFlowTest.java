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
import akka.stream.alpakka.mqtt.streaming.ConnAck;
import akka.stream.alpakka.mqtt.streaming.ConnAckFlags;
import akka.stream.alpakka.mqtt.streaming.ConnAckReturnCode;
import akka.stream.alpakka.mqtt.streaming.Connect;
import akka.stream.alpakka.mqtt.streaming.ConnectFlags;
import akka.stream.alpakka.mqtt.streaming.ControlPacket;
import akka.stream.alpakka.mqtt.streaming.ControlPacketFlags;
import akka.stream.alpakka.mqtt.streaming.DecodeErrorOrEvent;
import akka.stream.alpakka.mqtt.streaming.Event;
import akka.stream.alpakka.mqtt.streaming.MqttSessionSettings;
import akka.stream.alpakka.mqtt.streaming.PubAck;
import akka.stream.alpakka.mqtt.streaming.Publish;
import akka.stream.alpakka.mqtt.streaming.SubAck;
import akka.stream.alpakka.mqtt.streaming.Subscribe;
import akka.stream.alpakka.mqtt.streaming.javadsl.ActorMqttClientSession;
import akka.stream.alpakka.mqtt.streaming.javadsl.ActorMqttServerSession;
import akka.stream.alpakka.mqtt.streaming.javadsl.Mqtt;
import akka.stream.alpakka.mqtt.streaming.javadsl.MqttClientSession;
import akka.stream.alpakka.mqtt.streaming.javadsl.MqttServerSession;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.SourceQueueWithComplete;
import akka.stream.javadsl.Tcp;
import akka.stream.javadsl.BroadcastHub;
import akka.testkit.javadsl.TestKit;
import akka.util.ByteString;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;
import scala.collection.JavaConverters;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

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
  public void establishClientBidirectionalConnectionAndSubscribeToATopic()
      throws InterruptedException, ExecutionException, TimeoutException {
    String clientId = "source-spec/flow";
    String topic = "source-spec/topic1";

    // #create-streaming-flow
    MqttSessionSettings settings = MqttSessionSettings.create();
    MqttClientSession session = ActorMqttClientSession.create(settings, materializer, system);

    Flow<ByteString, ByteString, CompletionStage<Tcp.OutgoingConnection>> connection =
        Tcp.get(system).outgoingConnection("localhost", 1883);

    Flow<Command<Object>, DecodeErrorOrEvent<Object>, NotUsed> mqttFlow =
        Mqtt.clientSessionFlow(session).join(connection);
    // #create-streaming-flow

    // #run-streaming-flow
    Pair<SourceQueueWithComplete<Command<Object>>, CompletionStage<DecodeErrorOrEvent<Object>>>
        run =
            Source.<Command<Object>>queue(3, OverflowStrategy.fail())
                .via(mqttFlow)
                .drop(3)
                .toMat(Sink.head(), Keep.both())
                .run(materializer);

    SourceQueueWithComplete<Command<Object>> commands = run.first();
    commands.offer(new Command<>(new Connect(clientId, ConnectFlags.CleanSession())));
    commands.offer(new Command<>(new Subscribe(topic)));
    commands.offer(new Command<>(new Publish(topic, ByteString.fromString("ohi"))));
    // #run-streaming-flow

    CompletionStage<DecodeErrorOrEvent<Object>> event = run.second();
    DecodeErrorOrEvent<Object> decodeErrorOrEvent =
        event.toCompletableFuture().get(3, TimeUnit.SECONDS);
    assertTrue(decodeErrorOrEvent.getEvent().isPresent());
  }

  @Test
  public void establishServerBidirectionalConnectionAndSubscribeToATopic()
      throws InterruptedException, ExecutionException, TimeoutException {
    String clientId = "flow-spec/flow";
    String topic = "source-spec/topic1";
    String host = "localhost";
    int port = 9884;

    // #create-streaming-bind-flow
    MqttSessionSettings settings = MqttSessionSettings.create();
    MqttServerSession session = ActorMqttServerSession.create(settings, materializer, system);

    int maxConnections = 1;

    Source<DecodeErrorOrEvent, CompletionStage<Tcp.ServerBinding>> bindSource =
        Tcp.get(system)
            .bind(host, port)
            .flatMapMerge(
                maxConnections,
                connection -> {
                  Flow<Command<Object>, DecodeErrorOrEvent<Object>, NotUsed> mqttFlow =
                      Mqtt.serverSessionFlow(
                              session,
                              ByteString.fromArray(
                                  connection.remoteAddress().getAddress().getAddress()))
                          .join(connection.flow());

                  Pair<
                          SourceQueueWithComplete<Command<Object>>,
                          Source<DecodeErrorOrEvent<Object>, NotUsed>>
                      run =
                          Source.<Command<Object>>queue(2, OverflowStrategy.dropHead())
                              .via(mqttFlow)
                              .toMat(BroadcastHub.of(DecodeErrorOrEvent.class), Keep.both())
                              .run(materializer);

                  SourceQueueWithComplete<Command<Object>> queue = run.first();
                  Source<DecodeErrorOrEvent<Object>, NotUsed> source = run.second();

                  source.runForeach(
                      deOrE -> {
                        if (deOrE.getEvent().isPresent()) {
                          Event<Object> event = deOrE.getEvent().get();
                          ControlPacket cp = event.event();
                          if (cp instanceof Connect) {
                            queue.offer(
                                new Command<>(
                                    new ConnAck(
                                        ConnAckFlags.None(),
                                        ConnAckReturnCode.ConnectionAccepted())));
                          } else if (cp instanceof Subscribe) {
                            Subscribe subscribe = (Subscribe) cp;
                            Collection<Tuple2<String, ControlPacketFlags>> topicFilters =
                                JavaConverters.asJavaCollectionConverter(subscribe.topicFilters())
                                    .asJavaCollection();
                            List<Integer> flags =
                                topicFilters
                                    .stream()
                                    .map(x -> x._2().underlying())
                                    .collect(Collectors.toList());
                            queue.offer(new Command<>(new SubAck(subscribe.packetId(), flags)));
                          } else if (cp instanceof Publish) {
                            Publish publish = (Publish) cp;
                            int packetId = publish.packetId().get().underlying();
                            queue.offer(new Command<>(new PubAck(packetId)));
                            queue.offer(new Command<>(publish));
                          } // Ignore everything else
                        }
                      },
                      materializer);

                  return source;
                });
    // #create-streaming-bind-flow

    // #run-streaming-bind-flow
    CompletionStage<Tcp.ServerBinding> bound =
        bindSource.toMat(Sink.ignore(), Keep.left()).run(materializer);
    // #run-streaming-bind-flow

    bound.toCompletableFuture().get(3, TimeUnit.SECONDS);

    Flow<ByteString, ByteString, CompletionStage<Tcp.OutgoingConnection>> connection =
        Tcp.get(system).outgoingConnection(host, port);

    Flow<Command<Object>, DecodeErrorOrEvent<Object>, NotUsed> mqttFlow =
        Mqtt.clientSessionFlow(new ActorMqttClientSession(settings, materializer, system))
            .join(connection);

    Pair<SourceQueueWithComplete<Command<Object>>, CompletionStage<DecodeErrorOrEvent<Object>>>
        run =
            Source.<Command<Object>>queue(3, OverflowStrategy.fail())
                .via(mqttFlow)
                .drop(3)
                .toMat(Sink.head(), Keep.both())
                .run(materializer);

    SourceQueueWithComplete<Command<Object>> commands = run.first();
    commands.offer(new Command<>(new Connect(clientId, ConnectFlags.None())));
    commands.offer(new Command<>(new Subscribe(topic)));
    commands.offer(new Command<>(new Publish(topic, ByteString.fromString("ohi"))));

    CompletionStage<DecodeErrorOrEvent<Object>> event = run.second();
    DecodeErrorOrEvent decodeErrorOrEvent = event.toCompletableFuture().get(3, TimeUnit.SECONDS);
    assertTrue(decodeErrorOrEvent.getEvent().isPresent());
  }
}
