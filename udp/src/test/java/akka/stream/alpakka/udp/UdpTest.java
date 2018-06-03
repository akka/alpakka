/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.udp;

import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.udp.javadsl.Udp;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Source;
import akka.stream.testkit.TestPublisher;
import akka.stream.testkit.TestSubscriber;
import akka.stream.testkit.javadsl.TestSink;
import akka.stream.testkit.javadsl.TestSource;
import akka.testkit.javadsl.TestKit;
import akka.util.ByteString;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletionStage;

public class UdpTest {
  private static ActorSystem system;
  private static Materializer materializer;

  @BeforeClass
  public static void setup() {
    system = ActorSystem.create("UdpTest");
    materializer = ActorMaterializer.create(system);
  }

  @AfterClass
  public static void teardown() {
    TestKit.shutdownActorSystem(system);
  }

  @Test
  public void testSendAndReceiveMessages() throws Exception {
    final InetSocketAddress bindToLocal = new InetSocketAddress("localhost", 0);
    final Integer messagesToSend = 100;

    final Pair<Pair<TestPublisher.Probe<UdpMessage>, CompletionStage<InetSocketAddress>>, TestSubscriber.Probe<UdpMessage>> materialized =
      TestSource.<UdpMessage>probe(system)
        .viaMat(Udp.bindFlow(bindToLocal, system), Keep.both())
        .toMat(TestSink.probe(system), Keep.both())
        .run(materializer);

    final InetSocketAddress boundAddress = materialized.first().second().toCompletableFuture().get();

    final TestSubscriber.Probe<UdpMessage> sub = materialized.second();
    sub.ensureSubscription();
    sub.request(messagesToSend);

    Source.range(1, messagesToSend)
        .map(i -> ByteString.fromString("Message " + i))
        .map(bs -> UdpMessage.create(bs, boundAddress))
        .runWith(Udp.fireAndForgetSink(system), materializer);

    for (int i = 0; i < messagesToSend; i++) {
      sub.requestNext();
    }
    sub.cancel();
  }
}
