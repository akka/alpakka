/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.actor.ActorSystem;
import akka.io.Inet;
import akka.io.UdpSO;
import akka.japi.Pair;
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import akka.stream.alpakka.udp.Datagram;
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
import org.junit.Rule;
import org.junit.Test;

import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.StandardProtocolFamily;
import java.nio.channels.DatagramChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

public class UdpTest {
  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  private static ActorSystem system;

  @BeforeClass
  public static void setup() {
    system = ActorSystem.create("UdpTest");
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
    final Flow<Datagram, Datagram, CompletionStage<InetSocketAddress>> bindFlow =
        Udp.bindFlow(bindToLocal, system);
    // #bind-flow

    final Pair<
            Pair<TestPublisher.Probe<Datagram>, CompletionStage<InetSocketAddress>>,
            TestSubscriber.Probe<Datagram>>
        materialized =
            TestSource.<Datagram>probe(system)
                .viaMat(bindFlow, Keep.both())
                .toMat(TestSink.probe(system), Keep.both())
                .run(system);

    {
      // #send-datagrams
      final InetSocketAddress destination = new InetSocketAddress("my.server", 27015);
      // #send-datagrams
    }

    final InetSocketAddress destination = materialized.first().second().toCompletableFuture().get();

    // #send-datagrams
    final Integer messagesToSend = 100;

    // #send-datagrams

    final TestSubscriber.Probe<Datagram> sub = materialized.second();
    sub.ensureSubscription();
    sub.request(messagesToSend);

    // #send-datagrams
    Source.range(1, messagesToSend)
        .map(i -> ByteString.fromString("Message " + i))
        .map(bs -> Datagram.create(bs, destination))
        .runWith(Udp.sendSink(system), system);
    // #send-datagrams

    for (int i = 0; i < messagesToSend; i++) {
      sub.requestNext();
    }
    sub.cancel();
  }

  @Test
  public void testSendAndReceiveMessagesWithOptions() throws Exception {
    final InetSocketAddress bindToLocal = new InetSocketAddress("localhost", 0);

    final List<Inet.SocketOption> bindSocketOptions = new ArrayList<>();
    bindSocketOptions.add(UdpSO.broadcast(true));

    final Flow<Datagram, Datagram, CompletionStage<InetSocketAddress>> bindFlow =
        Udp.bindFlow(bindToLocal, bindSocketOptions, system);

    final Pair<
            Pair<TestPublisher.Probe<Datagram>, CompletionStage<InetSocketAddress>>,
            TestSubscriber.Probe<Datagram>>
        materialized =
            TestSource.<Datagram>probe(system)
                .viaMat(bindFlow, Keep.both())
                .toMat(TestSink.probe(system), Keep.both())
                .run(system);

    {
      final InetSocketAddress destination = new InetSocketAddress("my.server", 27015);
    }

    final InetSocketAddress destination = materialized.first().second().toCompletableFuture().get();

    final Integer messagesToSend = 100;

    final List<Inet.SocketOption> sendSocketOptions = new ArrayList<>();
    sendSocketOptions.add(UdpSO.broadcast(true));

    final TestSubscriber.Probe<Datagram> sub = materialized.second();
    sub.ensureSubscription();
    sub.request(messagesToSend);

    Source.range(1, messagesToSend)
        .map(i -> ByteString.fromString("Message " + i))
        .map(bs -> Datagram.create(bs, destination))
        .runWith(Udp.sendSink(sendSocketOptions, system), system);

    for (int i = 0; i < messagesToSend; i++) {
      sub.requestNext();
    }
    sub.cancel();
  }
}
