/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.unixdomainsocket.javadsl.UnixDomainSocket;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Framing;
import akka.stream.javadsl.FramingTruncation;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;
import akka.util.ByteString;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import akka.japi.Pair;

import java.io.File;
import java.nio.file.Files;
import java.util.concurrent.CompletionStage;

import akka.stream.alpakka.unixdomainsocket.javadsl.UnixDomainSocket.IncomingConnection;
import akka.stream.alpakka.unixdomainsocket.javadsl.UnixDomainSocket.ServerBinding;

public class UnixDomainSocketTest {

  private static ActorSystem system;
  private static Materializer materializer;

  private static Pair<ActorSystem, Materializer> setupMaterializer() {
    final ActorSystem system = ActorSystem.create();
    final Materializer materializer = ActorMaterializer.create(system);
    return Pair.create(system, materializer);
  }

  @BeforeClass
  public static void setup() throws Exception {
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
    java.io.File file = // ...
        // #binding
        Files.createTempFile("aUnixDomainSocketShouldReceiveWhatIsSent1", ".sock").toFile();
    Assert.assertTrue(file.delete());
    file.deleteOnExit();

    // #binding
    final Source<IncomingConnection, CompletionStage<ServerBinding>> connections =
        UnixDomainSocket.get(system).bind(file);
    // #binding

    // #outgoingConnection
    connections.runForeach(
        connection -> {
          System.out.println("New connection from: " + connection.remoteAddress());

          final Flow<ByteString, ByteString, NotUsed> echo =
              Flow.of(ByteString.class)
                  .via(
                      Framing.delimiter(
                          ByteString.fromString("\n"), 256, FramingTruncation.DISALLOW))
                  .map(ByteString::utf8String)
                  .map(s -> s + "!!!\n")
                  .map(ByteString::fromString);

          connection.handleWith(echo, materializer);
        },
        materializer);
    // #outgoingConnection
  }
}
