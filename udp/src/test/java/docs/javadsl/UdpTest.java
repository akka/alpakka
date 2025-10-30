/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
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
import java.net.*;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

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
            TestSource.<Datagram>create(system)
                .viaMat(bindFlow, Keep.both())
                .toMat(TestSink.create(system), Keep.both())
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

  List<InetAddress> listAllBroadcastAddresses() throws SocketException {
    List<InetAddress> broadcastList = new ArrayList<>();
    Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
    while (interfaces.hasMoreElements()) {
      NetworkInterface networkInterface = interfaces.nextElement();

      if (networkInterface.isLoopback() || !networkInterface.isUp()) {
        continue;
      }

      networkInterface.getInterfaceAddresses().stream()
          .map(a -> a.getBroadcast())
          .filter(Objects::nonNull)
          .forEach(broadcastList::add);
    }
    return broadcastList;
  }

  @Test
  public void testSendAndReceiveMessagesWithOptions() throws Exception {
    InetAddress firstBroadcastAddress = listAllBroadcastAddresses().get(0);
    final InetSocketAddress bindToBroadcast = new InetSocketAddress(firstBroadcastAddress, 0);

    final List<Inet.SocketOption> bindSocketOptions = new ArrayList<>();
    bindSocketOptions.add(UdpSO.broadcast(true));

    final Flow<Datagram, Datagram, CompletionStage<InetSocketAddress>> bindFlow =
        Udp.bindFlow(bindToBroadcast, bindSocketOptions, system);

    final Pair<
            Pair<TestPublisher.Probe<Datagram>, CompletionStage<InetSocketAddress>>,
            TestSubscriber.Probe<Datagram>>
        materialized =
            TestSource.<Datagram>create(system)
                .viaMat(bindFlow, Keep.both())
                .toMat(TestSink.create(system), Keep.both())
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
