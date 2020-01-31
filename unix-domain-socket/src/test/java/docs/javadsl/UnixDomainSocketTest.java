/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.*;
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import akka.stream.alpakka.unixdomainsocket.javadsl.UnixDomainSocket;
import akka.stream.javadsl.*;
import akka.testkit.javadsl.TestKit;
import akka.util.ByteString;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import akka.japi.Pair;

import java.nio.file.Files;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import akka.stream.alpakka.unixdomainsocket.javadsl.UnixDomainSocket.IncomingConnection;
import akka.stream.alpakka.unixdomainsocket.javadsl.UnixDomainSocket.ServerBinding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertTrue;

public class UnixDomainSocketTest {

  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  private static final Logger log = LoggerFactory.getLogger(UnixDomainSocketTest.class);
  private static ActorSystem system;
  private static Materializer materializer;
  private static int timeoutSeconds = 10;

  private static Pair<ActorSystem, Materializer> setupMaterializer() {
    final ActorSystem system = ActorSystem.create();
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

  @Test
  public void aUnixDomainSocketShouldReceiveWhatIsSent() throws Exception {
    // #binding
    java.nio.file.Path path = // ...
        // #binding
        Files.createTempDirectory("UnixDomainSocketSpec").resolve("sock1");
    path.toFile().deleteOnExit();

    // #binding
    final Source<IncomingConnection, CompletionStage<ServerBinding>> connections =
        UnixDomainSocket.get(system).bind(path);
    // #binding

    final CompletableFuture<ByteString> received = new CompletableFuture<>();

    // #outgoingConnection
    ByteString sendBytes = ByteString.fromString("Hello");
    CompletionStage<ServerBinding> streamCompletion =
        connections
            .map(
                connection -> {
                  log.info("New connection from: {}", connection.remoteAddress());

                  final Flow<ByteString, ByteString, NotUsed> echo =
                      Flow.of(ByteString.class)
                          // server logic ...
                          // #outgoingConnection
                          .buffer(1, OverflowStrategy.backpressure())
                          .wireTap(received::complete);
                  // #outgoingConnection

                  return connection.handleWith(echo, materializer);
                })
            .toMat(Sink.ignore(), Keep.left())
            .run(materializer);

    // #outgoingConnection
    Source.single(sendBytes)
        .via(UnixDomainSocket.get(system).outgoingConnection(path))
        .runWith(Sink.ignore(), materializer);

    assertTrue(received.get(timeoutSeconds, TimeUnit.SECONDS).equals(sendBytes));

    streamCompletion
        .toCompletableFuture()
        .get(timeoutSeconds, TimeUnit.SECONDS)
        .unbind()
        .toCompletableFuture()
        .get(timeoutSeconds, TimeUnit.SECONDS);
  }
}
