/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.JavaPartialFunction;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.KillSwitches;
import akka.stream.Materializer;
import akka.stream.OverflowStrategy;
import akka.stream.UniqueKillSwitch;
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
import akka.stream.testkit.javadsl.StreamTestKit;
import akka.testkit.javadsl.TestKit;
import akka.util.ByteString;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;
import scala.collection.JavaConverters;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class MqttFlowTest {

  private static int TIMEOUT_SECONDS = 5;

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

  @After
  public void assertStageStopping() {
    StreamTestKit.assertAllStagesStopped(materializer);
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
    Pair<SourceQueueWithComplete<Command<Object>>, CompletionStage<Publish>> run =
        Source.<Command<Object>>queue(3, OverflowStrategy.fail())
            .via(mqttFlow)
            .collect(
                new JavaPartialFunction<DecodeErrorOrEvent<Object>, Publish>() {
                  @Override
                  public Publish apply(DecodeErrorOrEvent<Object> x, boolean isCheck) {
                    if (x.getEvent().isPresent() && x.getEvent().get().event() instanceof Publish)
                      return (Publish) x.getEvent().get().event();
                    else throw noMatch();
                  }
                })
            .toMat(Sink.head(), Keep.both())
            .run(materializer);

    SourceQueueWithComplete<Command<Object>> commands = run.first();
    commands.offer(new Command<>(new Connect(clientId, ConnectFlags.CleanSession())));
    commands.offer(new Command<>(new Subscribe(topic)));
    session.tell(
        new Command<>(
            new Publish(
                ControlPacketFlags.RETAIN() | ControlPacketFlags.QoSAtLeastOnceDelivery(),
                topic,
                ByteString.fromString("ohi"))));
    // #run-streaming-flow

    CompletionStage<Publish> event = run.second();
    Publish publishEvent = event.toCompletableFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    assertEquals(publishEvent.topicName(), topic);
    assertEquals(publishEvent.payload(), ByteString.fromString("ohi"));

    // #run-streaming-flow

    // for shutting down properly
    commands.complete();
    session.shutdown();
    // #run-streaming-flow
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

    Source<DecodeErrorOrEvent<Object>, CompletionStage<Tcp.ServerBinding>> bindSource =
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
                              .toMat(BroadcastHub.of(DecodeErrorOrEvent.classOf()), Keep.both())
                              .run(materializer);

                  SourceQueueWithComplete<Command<Object>> queue = run.first();
                  Source<DecodeErrorOrEvent<Object>, NotUsed> source = run.second();

                  CompletableFuture<Done> subscribed = new CompletableFuture<>();
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
                            queue.offer(
                                new Command<>(
                                    new SubAck(subscribe.packetId(), flags),
                                    Optional.of(subscribed),
                                    Optional.empty()));
                          } else if (cp instanceof Publish) {
                            Publish publish = (Publish) cp;
                            if ((publish.flags() & ControlPacketFlags.RETAIN()) != 0) {
                              int packetId = publish.packetId().get().underlying();
                              queue.offer(new Command<>(new PubAck(packetId)));
                              subscribed.thenRun(() -> session.tell(new Command<>(publish)));
                            }
                          } // Ignore everything else
                        }
                      },
                      materializer);

                  return source;
                });
    // #create-streaming-bind-flow

    // #run-streaming-bind-flow
    Pair<CompletionStage<Tcp.ServerBinding>, UniqueKillSwitch> bindingAndSwitch =
        bindSource.viaMat(KillSwitches.single(), Keep.both()).to(Sink.ignore()).run(materializer);

    CompletionStage<Tcp.ServerBinding> bound = bindingAndSwitch.first();
    UniqueKillSwitch server = bindingAndSwitch.second();
    // #run-streaming-bind-flow

    bound.toCompletableFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

    Flow<ByteString, ByteString, CompletionStage<Tcp.OutgoingConnection>> connection =
        Tcp.get(system).outgoingConnection(host, port);

    MqttClientSession clientSession = new ActorMqttClientSession(settings, materializer, system);

    Flow<Command<Object>, DecodeErrorOrEvent<Object>, NotUsed> mqttFlow =
        Mqtt.clientSessionFlow(clientSession).join(connection);

    Pair<SourceQueueWithComplete<Command<Object>>, CompletionStage<Publish>> run =
        Source.<Command<Object>>queue(3, OverflowStrategy.fail())
            .via(mqttFlow)
            .collect(
                new JavaPartialFunction<DecodeErrorOrEvent<Object>, Publish>() {
                  @Override
                  public Publish apply(DecodeErrorOrEvent<Object> x, boolean isCheck) {
                    if (x.getEvent().isPresent() && x.getEvent().get().event() instanceof Publish)
                      return (Publish) x.getEvent().get().event();
                    else throw noMatch();
                  }
                })
            .toMat(Sink.head(), Keep.both())
            .run(materializer);

    SourceQueueWithComplete<Command<Object>> commands = run.first();
    commands.offer(new Command<>(new Connect(clientId, ConnectFlags.None())));
    commands.offer(new Command<>(new Subscribe(topic)));
    clientSession.tell(
        new Command<>(
            new Publish(
                ControlPacketFlags.RETAIN() | ControlPacketFlags.QoSAtLeastOnceDelivery(),
                topic,
                ByteString.fromString("ohi"))));

    CompletionStage<Publish> event = run.second();
    Publish publish = event.toCompletableFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    assertEquals(publish.topicName(), topic);
    assertEquals(publish.payload(), ByteString.fromString("ohi"));

    // #run-streaming-bind-flow

    // for shutting down properly
    server.shutdown();
    session.shutdown();
    // #run-streaming-bind-flow
  }
}
