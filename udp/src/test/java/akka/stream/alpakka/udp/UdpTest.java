/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.udp;

import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.udp.javadsl.Udp;
import akka.stream.javadsl.Flow;
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
    // #bind-address
    final InetSocketAddress bindToLocal = new InetSocketAddress("localhost", 0);
    // #bind-address

    // #bind-flow
    final Flow<UdpMessage, UdpMessage, CompletionStage<InetSocketAddress>> bindFlow =
      Udp.bindFlow(bindToLocal, system);
    // #bind-flow

    final Pair<Pair<TestPublisher.Probe<UdpMessage>, CompletionStage<InetSocketAddress>>, TestSubscriber.Probe<UdpMessage>> materialized =
      TestSource.<UdpMessage>probe(system)
        .viaMat(bindFlow, Keep.both())
        .toMat(TestSink.probe(system), Keep.both())
        .run(materializer);

    {
      // #send-messages
      final InetSocketAddress destination = new InetSocketAddress("my.server", 27015);
      // #send-messages
    }

    final InetSocketAddress destination = materialized.first().second().toCompletableFuture().get();

    // #send-messages
    final Integer messagesToSend = 100;

    // #send-messages

    final TestSubscriber.Probe<UdpMessage> sub = materialized.second();
    sub.ensureSubscription();
    sub.request(messagesToSend);

    // #send-messages
    Source.range(1, messagesToSend)
        .map(i -> ByteString.fromString("Message " + i))
        .map(bs -> UdpMessage.create(bs, destination))
        .runWith(Udp.sendSink(system), materializer);
    // #send-messages

    for (int i = 0; i < messagesToSend; i++) {
      sub.requestNext();
    }
    sub.cancel();
  }
}
