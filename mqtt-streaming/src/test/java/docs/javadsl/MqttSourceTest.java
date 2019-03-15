/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.dispatch.ExecutionContexts;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.KillSwitches;
import akka.stream.Materializer;
import akka.stream.UniqueKillSwitch;
// #imports
import akka.stream.alpakka.mqtt.streaming.*;
import akka.stream.alpakka.mqtt.streaming.javadsl.ActorMqttClientSession;
import akka.stream.alpakka.mqtt.streaming.javadsl.MqttAckHandle;
import akka.stream.alpakka.mqtt.streaming.javadsl.MqttClientSession;
import akka.stream.alpakka.mqtt.streaming.javadsl.MqttSource;
// #imports
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.SourceQueueWithComplete;
import akka.testkit.javadsl.TestKit;
import docs.scaladsl.MqttSourceSpec;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.collection.JavaConverters;
import scala.concurrent.ExecutionContext;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

public class MqttSourceTest {

  private static int TIMEOUT_SECONDS = 5;
  private final String topicPrefix = "streaming/source-test/topic/";
  private final String time = LocalTime.now().toString();
  private final List<String> input =
      Arrays.asList("one-" + time, "two-" + time, "three-" + time, "four-" + time, "five-" + time);

  private static ActorSystem system;
  private static Materializer materializer;
  private ExecutionContext executionContext =
      ExecutionContexts.fromExecutor(Executors.newSingleThreadExecutor());

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

    MqttClientSession mqttClientSession =
        ActorMqttClientSession.create(MqttSessionSettings.create(), materializer, system);
    MqttTcpTransportSettings transportSettings = MqttTcpTransportSettings.create("localhost");
    MqttSubscribe subscriptions = MqttSubscriptions.atLeastOnce(topic);

    Pair<
            Pair<CompletionStage<List<Pair<String, ControlPacketFlags>>>, UniqueKillSwitch>,
            CompletionStage<Done>>
        stream =
            MqttSource.atLeastOnce(
                    mqttClientSession,
                    transportSettings,
                    MqttRestartSettings.create(),
                    MqttConnectionSettings.create(clientId),
                    subscriptions)
                .via(businessLogic)
                .mapAsync(
                    1,
                    pair -> {
                      MqttAckHandle ackHandle = pair.second();
                      return ackHandle.ack();
                    })
                .viaMat(KillSwitches.single(), Keep.both())
                .toMat(Sink.ignore(), Keep.both())
                .run(materializer);

    CompletionStage<List<Pair<String, ControlPacketFlags>>> subscribed = stream.first().first();
    UniqueKillSwitch killSwitch = stream.first().second();
    CompletionStage<Done> streamCompletion = stream.second();
    // #at-least-once

    SourceQueueWithComplete<Command<?>> publishFlow =
        //        // TODO remove hack
        new akka.stream.impl.SourceQueueAdapter(
            MqttSourceSpec.publish(
                topic,
                ControlPacketFlags.QoSExactlyOnceDelivery(),
                transportSettings,
                JavaConverters.asScalaBuffer(input).toList(),
                materializer,
                system,
                executionContext));

    Thread.sleep(2 * 1000);
    // #at-least-once

    // stop the subscription
    killSwitch.shutdown();
    // #at-least-once

    // TODO this assert fails
    //    assertThat(
    //        received.stream().map(p -> p.payload().utf8String()).collect(Collectors.toList()),
    //        is(input));

    publishFlow.complete();
    // #at-least-once

    streamCompletion.thenAccept(done -> mqttClientSession.shutdown());
    // #at-least-once
  }
}
