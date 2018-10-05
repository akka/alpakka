/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

/*
 * Start package with 'docs' prefix when testing APIs as a user.
 * This prevents any visibility issues that may be hidden.
 */
package docs.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import static akka.japi.Util.classTag;
import static org.junit.Assert.assertEquals;

import akka.japi.Pair;
import akka.pattern.Patterns;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.OverflowStrategy;
import akka.stream.alpakka.mqtt.streaming.Command;
import akka.stream.alpakka.mqtt.streaming.ConnAck;
import akka.stream.alpakka.mqtt.streaming.ConnAckFlags;
import akka.stream.alpakka.mqtt.streaming.ConnAckReturnCode;
import akka.stream.alpakka.mqtt.streaming.Connect;
import akka.stream.alpakka.mqtt.streaming.ConnectFlags;
import akka.stream.alpakka.mqtt.streaming.ControlPacketFlags;
import akka.stream.alpakka.mqtt.streaming.Event;
import akka.stream.alpakka.mqtt.streaming.DecodeErrorOrEvent;
import akka.stream.alpakka.mqtt.streaming.MqttCodec;
import akka.stream.alpakka.mqtt.streaming.PacketId;
import akka.stream.alpakka.mqtt.streaming.PubAck;
import akka.stream.alpakka.mqtt.streaming.Publish;
import akka.stream.alpakka.mqtt.streaming.MqttSessionSettings;
import akka.stream.alpakka.mqtt.streaming.SubAck;
import akka.stream.alpakka.mqtt.streaming.Subscribe;
import akka.stream.alpakka.mqtt.streaming.javadsl.ActorMqttClientSession;
import akka.stream.alpakka.mqtt.streaming.javadsl.Mqtt;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.SourceQueueWithComplete;
import akka.testkit.TestProbe;
import akka.testkit.javadsl.TestKit;
import akka.util.ByteString;
import akka.util.Timeout;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.compat.java8.FutureConverters;
import scala.compat.java8.OptionConverters;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MqttTest {
  private static ActorSystem system;
  private static Materializer mat;

  private static Timeout timeout;

  private static ActorMqttClientSession session;

  @BeforeClass
  public static void setUpBeforeClass() {
    system = ActorSystem.create("MqttTestJava");
    mat = ActorMaterializer.create(system);

    timeout = new Timeout(3, TimeUnit.SECONDS);
    Duration timeoutDuration = Duration.of(timeout.duration().toSeconds(), ChronoUnit.SECONDS);

    MqttSessionSettings settings =
        MqttSessionSettings.create(
            100,
            100,
            timeoutDuration,
            1,
            1,
            timeoutDuration,
            timeoutDuration,
            timeoutDuration,
            timeoutDuration,
            timeoutDuration);
    session = new ActorMqttClientSession(settings, system);
  }

  @Test
  public void flowThroughAClientSession()
      throws InterruptedException, ExecutionException, TimeoutException {
    new TestKit(system) {
      {
        TestProbe server = new TestProbe(system);
        Flow<ByteString, ByteString, NotUsed> pipeToServer =
            Flow.of(ByteString.class)
                .mapAsync(
                    1,
                    msg ->
                        FutureConverters.toJava(
                            Patterns.ask(server.ref(), msg, timeout)
                                .mapTo(classTag(ByteString.class))));

        Pair<SourceQueueWithComplete<Command<?>>, CompletionStage<List<DecodeErrorOrEvent>>>
            resultPair =
                Source.<Command<?>>queue(1, OverflowStrategy.fail())
                    .via(Mqtt.clientSessionFlow(session).join(pipeToServer))
                    .toMat(Sink.seq(), Keep.both())
                    .run(mat);

        SourceQueueWithComplete<Command<?>> client = resultPair.first();
        CompletionStage<List<DecodeErrorOrEvent>> result = resultPair.second();

        Connect connect = new Connect("some-client-id", ConnectFlags.None());

        ByteString connectBytes =
            new MqttCodec.MqttConnect(connect).encode(ByteString.createBuilder()).result();
        ConnAck connAck = new ConnAck(ConnAckFlags.None(), ConnAckReturnCode.ConnectionAccepted());
        ByteString connAckBytes =
            new MqttCodec.MqttConnAck(connAck).encode(ByteString.createBuilder()).result();

        Subscribe subscribe = new Subscribe("some-topic");
        ByteString subscribeBytes =
            new MqttCodec.MqttSubscribe(subscribe).encode(ByteString.createBuilder(), 1).result();
        List<Integer> collect =
            Stream.of(ControlPacketFlags.QoSAtLeastOnceDelivery()).collect(Collectors.toList());
        SubAck subAck = new SubAck(1, collect);
        ByteString subAckBytes =
            new MqttCodec.MqttSubAck(subAck).encode(ByteString.createBuilder()).result();

        Publish publish = new Publish("some-topic", ByteString.fromString("some-payload"));
        ByteString publishBytes =
            new MqttCodec.MqttPublish(publish)
                .encode(
                    ByteString.createBuilder(),
                    OptionConverters.toScala(Optional.of(new PacketId(1))))
                .result();
        PubAck pubAck = new PubAck(1);
        ByteString pubAckBytes =
            new MqttCodec.MqttPubAck(pubAck).encode(ByteString.createBuilder()).result();

        client.offer(new Command(connect));

        server.expectMsg(connectBytes);
        server.reply(connAckBytes);

        client.offer(new Command(subscribe));

        server.expectMsg(subscribeBytes);
        server.reply(subAckBytes);

        client.offer(new Command(publish));

        server.expectMsg(publishBytes);
        server.reply(pubAckBytes);

        client.complete();

        assertEquals(
            result
                .thenApply(
                    e -> e.stream().map(DecodeErrorOrEvent::getEvent).collect(Collectors.toList()))
                .toCompletableFuture()
                .get(timeout.duration().toSeconds(), TimeUnit.SECONDS),
            Stream.of(
                    Optional.of(new Event(connAck)),
                    Optional.of(new Event(subAck)),
                    Optional.of(new Event(pubAck)))
                .collect(Collectors.toList()));
      }
    };
  }

  @AfterClass
  public static void tearDownAfterClass() {
    TestKit.shutdownActorSystem(system);
  }
}
