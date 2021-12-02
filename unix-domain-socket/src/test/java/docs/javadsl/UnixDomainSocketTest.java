/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.Materializer;
import akka.stream.OverflowStrategy;
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import akka.stream.alpakka.unixdomainsocket.javadsl.UnixDomainSocket;
import akka.stream.alpakka.unixdomainsocket.javadsl.UnixDomainSocket.IncomingConnection;
import akka.stream.alpakka.unixdomainsocket.javadsl.UnixDomainSocket.ServerBinding;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;
import akka.util.ByteString;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class UnixDomainSocketTest {

  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  private static final Logger log = LoggerFactory.getLogger(UnixDomainSocketTest.class);
  private static ActorSystem system;
  private static Materializer materializer;
  private static final int timeoutSeconds = 10;

  private static Pair<ActorSystem, Materializer> setupMaterializer() {
    final ActorSystem system = ActorSystem.create();
    final Materializer materializer = Materializer.createMaterializer(system);
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
        Files.createTempDirectory("UnixDomainSocketTest").resolve("sock1");
    path.toFile().deleteOnExit();

    // #binding
    final Source<IncomingConnection, CompletionStage<ServerBinding>> connections =
        UnixDomainSocket.get(system).bind(path);
    // #binding

    final CompletableFuture<ByteString> received = new CompletableFuture<>();

    // #outgoingConnection
    CompletionStage<ServerBinding> futureBinding =
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
            .run(system);

    // #outgoingConnection

    ServerBinding serverBinding =
        futureBinding.toCompletableFuture().get(timeoutSeconds, TimeUnit.SECONDS);

    final ByteString sendBytes = ByteString.fromString("Hello");
    final CompletionStage<Done> sent =
        Source.single(sendBytes)
            .via(UnixDomainSocket.get(system).outgoingConnection(path))
            .runWith(Sink.ignore(), system);

    sent.toCompletableFuture().get(timeoutSeconds, TimeUnit.SECONDS);
    assertEquals(sendBytes, received.get(timeoutSeconds, TimeUnit.SECONDS));

    serverBinding.unbind().toCompletableFuture().get(timeoutSeconds, TimeUnit.SECONDS);
  }
}
