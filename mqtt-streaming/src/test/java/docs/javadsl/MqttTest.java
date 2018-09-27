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

import akka.pattern.Patterns;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.mqtt.streaming.ConnAck;
import akka.stream.alpakka.mqtt.streaming.ConnAckFlags;
import akka.stream.alpakka.mqtt.streaming.ConnAckReturnCode;
import akka.stream.alpakka.mqtt.streaming.Connect;
import akka.stream.alpakka.mqtt.streaming.ConnectFlags;
import akka.stream.alpakka.mqtt.streaming.ControlPacket;
import akka.stream.alpakka.mqtt.streaming.ControlPacketFlags;
import akka.stream.alpakka.mqtt.streaming.MqttCodec;
import akka.stream.alpakka.mqtt.streaming.PacketId;
import akka.stream.alpakka.mqtt.streaming.Publish;
import akka.stream.alpakka.mqtt.streaming.SessionFlowSettings;
import akka.stream.alpakka.mqtt.streaming.SubAck;
import akka.stream.alpakka.mqtt.streaming.Subscribe;
import akka.stream.alpakka.mqtt.streaming.javadsl.Mqtt;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.TestProbe;
import akka.testkit.javadsl.TestKit;
import akka.util.ByteString;
import akka.util.Timeout;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.compat.java8.FutureConverters;
import scala.compat.java8.OptionConverters;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MqttTest {
  private static ActorSystem sys;
  private static Materializer mat;

  private Timeout timeout = new Timeout(3, TimeUnit.SECONDS);

  private SessionFlowSettings settings = SessionFlowSettings.create(100);

  @BeforeClass
  public static void setUpBeforeClass() {
    sys = ActorSystem.create("MqttTestJava");
    mat = ActorMaterializer.create(sys);
  }

  @Test
  public void flowThroughASession()
      throws InterruptedException, ExecutionException, TimeoutException {
    new TestKit(sys) {
      {
        TestProbe server = new TestProbe(sys);
        Flow<ByteString, ByteString, NotUsed> pipeToServer =
            Flow.of(ByteString.class)
                .mapAsync(
                    1,
                    msg ->
                        FutureConverters.toJava(
                            Patterns.ask(server.ref(), msg, timeout)
                                .mapTo(classTag(ByteString.class))));

        Connect connect = new Connect("some-client-id", ConnectFlags.None());
        Subscribe subscribe = new Subscribe("some-topic");

        CompletionStage<List<MqttCodec.ControlPacketResult>> result =
            Source.from(Stream.<ControlPacket>of(connect, subscribe).collect(Collectors.toList()))
                .via(Mqtt.sessionFlow(settings).join(pipeToServer))
                .runWith(Sink.seq(), mat);

        ByteString connectBytes =
            new MqttCodec.MqttConnect(connect).encode(ByteString.createBuilder()).result();
        ConnAck connAck = new ConnAck(ConnAckFlags.None(), ConnAckReturnCode.ConnectionAccepted());
        ByteString connAckBytes =
            new MqttCodec.MqttConnAck(connAck).encode(ByteString.createBuilder()).result();

        ByteString subscribeBytes =
            new MqttCodec.MqttSubscribe(subscribe).encode(ByteString.createBuilder(), 0).result();
        List<Integer> collect =
            Stream.of(ControlPacketFlags.QoSAtLeastOnceDelivery()).collect(Collectors.toList());
        SubAck subAck = new SubAck(0, collect);
        ByteString subAckBytes =
            new MqttCodec.MqttSubAck(subAck).encode(ByteString.createBuilder()).result();

        Publish publish = new Publish("some-topic", ByteString.fromString("some-payload"));
        ByteString publishBytes =
            new MqttCodec.MqttPublish(publish)
                .encode(
                    ByteString.createBuilder(),
                    OptionConverters.toScala(Optional.of(new PacketId(0))))
                .result();

        server.expectMsg(connectBytes);
        server.reply(connAckBytes);

        server.expectMsg(subscribeBytes);
        server.reply(subAckBytes.concat(publishBytes));

        assertEquals(
            result
                .thenApply(
                    e ->
                        e.stream()
                            .map(MqttCodec.ControlPacketResult::getControlPacket)
                            .collect(Collectors.toList()))
                .toCompletableFuture()
                .get(timeout.duration().toSeconds(), TimeUnit.SECONDS),
            Stream.of(Optional.of(connAck), Optional.of(subAck), Optional.of(publish))
                .collect(Collectors.toList()));
      }
    };
  }

  @AfterClass
  public static void tearDownAfterClass() {
    TestKit.shutdownActorSystem(sys);
  }
}
