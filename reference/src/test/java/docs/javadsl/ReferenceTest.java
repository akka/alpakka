/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

/*
 * Start package with 'docs' prefix when testing APIs as a user.
 * This prevents any visibility issues that may be hidden.
 */
package docs.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.reference.Authentication;
import akka.stream.alpakka.reference.ReferenceReadMessage;
import akka.stream.alpakka.reference.ReferenceWriteMessage;
import akka.stream.alpakka.reference.SourceSettings;
import akka.stream.alpakka.reference.javadsl.Reference;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;
import akka.util.ByteString;
import org.junit.*;

import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/** Append "Test" to every Java test suite. */
public class ReferenceTest {

  static ActorSystem sys;
  static Materializer mat;

  static final String clientId = "test-client-id";

  /** Called before test suite. */
  @BeforeClass
  public static void setUpBeforeClass() {
    sys = ActorSystem.create("ReferenceTest");
    mat = ActorMaterializer.create(sys);
  }

  /** Called before every test. */
  @Before
  public void setUp() {}

  @Test
  public void settingsCompilationTest() {
    final Authentication.Provided providedAuth =
        Authentication.createProvided().withVerifierPredicate(c -> true);

    final Authentication.None noAuth = Authentication.createNone();

    final SourceSettings settings = SourceSettings.create(clientId);

    settings.withAuthentication(providedAuth);
    settings.withAuthentication(noAuth);
  }

  @Test
  public void sourceCompilationTest() {
    // #source
    final SourceSettings settings = SourceSettings.create(clientId);

    final Source<ReferenceReadMessage, CompletionStage<Done>> source = Reference.source(settings);
    // #source
  }

  @Test
  public void flowCompilationTest() {
    // #flow
    final Flow<ReferenceWriteMessage, ReferenceWriteMessage, NotUsed> flow = Reference.flow();
    // #flow

    final Executor ex = Executors.newCachedThreadPool();
    final Flow<ReferenceWriteMessage, ReferenceWriteMessage, NotUsed> flow2 =
        Reference.flowAsyncMapped(ex);
  }

  @Test
  public void testSource() throws Exception {
    final Source<ReferenceReadMessage, CompletionStage<Done>> source =
        Reference.source(SourceSettings.create(clientId));

    final CompletionStage<ReferenceReadMessage> stage = source.runWith(Sink.head(), mat);
    final ReferenceReadMessage msg = stage.toCompletableFuture().get();

    Assert.assertEquals(Collections.singletonList(ByteString.fromString("one")), msg.getData());

    final Integer expected = 100;
    Assert.assertEquals(expected, msg.getBytesRead());

    Assert.assertNull(msg.getBytesReadFailure());
  }

  @Test
  public void testFlow() throws Exception {
    final Flow<ReferenceWriteMessage, ReferenceWriteMessage, NotUsed> flow = Reference.flow();

    Map<String, Long> metrics =
        new HashMap<String, Long>() {
          {
            put("rps", 20L);
            put("rpm", Long.valueOf(30L));
          }
        };

    final Source<ReferenceWriteMessage, NotUsed> source =
        Source.from(
            Arrays.asList(
                ReferenceWriteMessage.create()
                    .withData(Collections.singletonList(ByteString.fromString("one")))
                    .withMetrics(metrics),
                ReferenceWriteMessage.create()
                    .withData(
                        Arrays.asList(
                            ByteString.fromString("two"),
                            ByteString.fromString("three"),
                            ByteString.fromString("four")))));

    final CompletionStage<List<ReferenceWriteMessage>> stage =
        source.via(flow).runWith(Sink.seq(), mat);
    final List<ReferenceWriteMessage> result = stage.toCompletableFuture().get();

    final List<ByteString> bytes =
        result.stream().flatMap(m -> m.getData().stream()).collect(Collectors.toList());

    Assert.assertEquals(
        Arrays.asList(
            ByteString.fromString("one"),
            ByteString.fromString("two"),
            ByteString.fromString("three"),
            ByteString.fromString("four")),
        bytes);

    final long actual = result.stream().findFirst().get().getMetrics().get("total");
    Assert.assertEquals(50L, actual);
  }

  /** Called after every test. */
  @After
  public void tearDown() {}

  /** Called after test suite. */
  @AfterClass
  public static void tearDownAfterClass() {
    TestKit.shutdownActorSystem(sys);
  }
}
