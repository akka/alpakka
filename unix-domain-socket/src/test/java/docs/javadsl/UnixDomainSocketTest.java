/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.KillSwitch;
import akka.stream.KillSwitches;
import akka.stream.Materializer;
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
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import akka.stream.alpakka.unixdomainsocket.javadsl.UnixDomainSocket.IncomingConnection;
import akka.stream.alpakka.unixdomainsocket.javadsl.UnixDomainSocket.ServerBinding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnixDomainSocketTest {

  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  private static final Logger log = LoggerFactory.getLogger(UnixDomainSocketTest.class);
  private static ActorSystem system;
  private static Materializer materializer;

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
        Files.createTempFile("aUnixDomainSocketShouldReceiveWhatIsSent1", ".sock");
    path.toFile().deleteOnExit();

    // #binding
    final Source<IncomingConnection, CompletionStage<ServerBinding>> connections =
        UnixDomainSocket.get(system).bind(path);
    // #binding

    // #outgoingConnection
    CompletionStage<ServerBinding> streamCompletion =
        connections
            .map(
                connection -> {
                  log.info("New connection from: {}", connection.remoteAddress());

                  final Flow<ByteString, ByteString, NotUsed> echo =
                      Flow.of(ByteString.class)
                          .via(
                              Framing.delimiter(
                                  ByteString.fromString("\n"), 256, FramingTruncation.DISALLOW))
                          .map(ByteString::utf8String)
                          .map(s -> s + "!!!\n")
                          .map(ByteString::fromString);

                  return connection.handleWith(echo, materializer);
                })
            .toMat(Sink.ignore(), Keep.left())
            .run(materializer);
    // #outgoingConnection
    streamCompletion
        .toCompletableFuture()
        .get(2, TimeUnit.SECONDS)
        .unbind()
        .toCompletableFuture()
        .get(5, TimeUnit.SECONDS);
  }
}
