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
import akka.pattern.Patterns;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.mqtt.streaming.Connect;
import akka.stream.alpakka.mqtt.streaming.ConnectFlags;
import akka.stream.alpakka.mqtt.streaming.ControlPacket;
import akka.stream.alpakka.mqtt.streaming.MqttCodec;
import akka.stream.alpakka.mqtt.streaming.SessionFlowSettings;
import akka.stream.alpakka.mqtt.streaming.Subscribe;
import akka.stream.alpakka.mqtt.streaming.javadsl.Mqtt;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Source;
import akka.testkit.TestProbe;
import akka.testkit.javadsl.TestKit;
import akka.util.ByteString;
import akka.util.Timeout;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.compat.java8.FutureConverters;
import scala.util.Either;

import java.util.concurrent.TimeUnit;
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
  public void flowThroughASession() {
    new TestKit(sys) {
      {
        TestProbe server = new TestProbe(sys);
        Flow<ByteString, Either<MqttCodec.DecodeError, ControlPacket>, NotUsed> pipeToServer =
            Flow.of(ByteString.class)
                .mapAsync(
                    1,
                    msg ->
                        FutureConverters.toJava(
                            Patterns.ask(server.ref(), msg, timeout)
                                .mapTo(classTag(Either<MqttCodec.DecodeError, ControlPacket>.class))));

        Connect connect = Connect.create("some-client-id", ConnectFlags.None());
        Subscribe subscribe = Subscribe.create(0, "some-topic");

        Source.from(Stream.of(connect, subscribe).collect(Collectors.toList()))
            .via(Mqtt.sessionFlow(settings).join(pipeToServer));
      }
    };
  }

  @AfterClass
  public static void tearDownAfterClass() {
    TestKit.shutdownActorSystem(sys);
  }
}
