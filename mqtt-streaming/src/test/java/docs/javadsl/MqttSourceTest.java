/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Pair;
import akka.stream.*;
// #imports
import akka.stream.alpakka.mqtt.streaming.*;
import akka.stream.alpakka.mqtt.streaming.javadsl.*;
// #imports
import akka.stream.javadsl.*;
import akka.testkit.javadsl.TestKit;
import akka.util.ByteString;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class MqttSourceTest {

  private final String topicPrefix = "streaming/source-test/topic/";
  private final String time = LocalTime.now().toString();
  private final List<String> input =
      Arrays.asList("one-" + time, "two-" + time, "three-" + time, "four-" + time, "five-" + time);

  private static ActorSystem system;
  private static Materializer materializer;
  private final LoggingAdapter logger = Logging.getLogger(system, this);

  @BeforeClass
  public static void setup() {
    MqttSourceTest.system = ActorSystem.create("MqttSourceTest");
    MqttSourceTest.materializer = ActorMaterializer.create(system);
  }

  @AfterClass
  public static void teardown() {
    TestKit.shutdownActorSystem(MqttSourceTest.system);
  }

  @After
  public void assertStageStopping() {
    //    StreamTestKit.assertAllStagesStopped(materializer);
  }

  @Test
  public void atLeastOnceSubscription()
      throws InterruptedException, ExecutionException, TimeoutException {
    String testId = "1";
    String clientId = "streaming/source-test/" + testId;
    String topic = topicPrefix + testId;

    List<Publish> received = new ArrayList<>();

    // #at-least-once

    Flow<Pair<Publish, MqttAckHandle>, Pair<Publish, MqttAckHandle>, NotUsed> businessLogic = // ???
        // #at-least-once
        Flow.<Pair<Publish, MqttAckHandle>>create()
            .map(
                pair -> {
                  received.add(pair.first());
                  return pair;
                });

    // #at-least-once

    MqttTcpTransportSettings transportSettings = MqttTcpTransportSettings.create("localhost");
    MqttSubscribe subscriptions = MqttSubscriptions.atLeastOnce(topic);

    Pair<
            Pair<CompletionStage<List<Pair<String, ControlPacketFlags>>>, UniqueKillSwitch>,
            CompletionStage<Done>>
        stream =
            MqttSource.atLeastOnce(
                    MqttSessionSettings.create(),
                    transportSettings,
                    MqttRestartSettings.create(),
                    MqttConnectionSettings.create(clientId),
                    subscriptions)
                .log("start", logger)
                .via(businessLogic)
                .mapAsync(
                    1,
                    pair -> {
                      MqttAckHandle ackHandle = pair.second();
                      return ackHandle.ack().thenApply(done -> pair.first());
                    })
                .viaMat(KillSwitches.single(), Keep.both())
                .toMat(Sink.ignore(), Keep.both())
                .run(materializer);

    CompletionStage<List<Pair<String, ControlPacketFlags>>> subscribed = stream.first().first();
    UniqueKillSwitch killSwitch = stream.first().second();
    CompletionStage<Done> streamCompletion = stream.second();
    // #at-least-once

    SourceQueueWithComplete<Command<Object>> publishFlow =
        publish(
            topic,
            ControlPacketFlags.QoSAtLeastOnceDelivery(),
            transportSettings,
            input,
            system,
            materializer);

    Thread.sleep(2 * 1000);
    // #at-least-once

    // stop the subscription
    killSwitch.shutdown();
    // #at-least-once

    assertThat(
        received.stream().map(p -> p.payload().utf8String()).collect(Collectors.toList()),
        is(input));

    publishFlow.complete();
  }

  private static SourceQueueWithComplete<Command<Object>> publish(
      String topic,
      int controlPacketFlags,
      MqttTransportSettings transportSettings,
      Collection<String> input,
      ActorSystem actorSystem,
      Materializer materializer) {
    String clientId = "streaming/source-test/sender";
    MqttClientSession session =
        new ActorMqttClientSession(MqttSessionSettings.create(), materializer, actorSystem);

    Flow<Command<Object>, DecodeErrorOrEvent<Object>, NotUsed> join =
        Mqtt.clientSessionFlow(session, ByteString.fromString("SourceQueueWithComplete"))
            .join(transportSettings.connectionFlow(actorSystem).asJava());
    SourceQueueWithComplete<Command<Object>> commands =
        Source.<Command<Object>>queue(10, OverflowStrategy.fail())
            .prepend(
                Source.from(
                    Collections.singletonList(
                        new Command<>(new Connect(clientId, ConnectFlags.CleanSession())))))
            .via(join)
            .to(Sink.ignore())
            .run(materializer);
    input.forEach(
        d -> {
          session.tell(
              new Command<>(new Publish(controlPacketFlags, topic, ByteString.fromString(d))));
        });

    commands.watchCompletion().thenAccept(done -> session.shutdown());

    return commands;
  }
}
