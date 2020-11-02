/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

/*
 * Start package with 'docs' prefix when testing APIs as a user.
 * This prevents any visibility issues that may be hidden.
 */
package docs.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.alpakka.reference.*;
import akka.stream.alpakka.reference.javadsl.Reference;
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** Append "Test" to every Java test suite. */
public class ReferenceTest {
  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  static ActorSystem system;

  static final String clientId = "test-client-id";

  /** Called before test suite. */
  @BeforeClass
  public static void setUpBeforeClass() {
    system = ActorSystem.create("ReferenceTest");
  }

  /** Called before every test. */
  @Before
  public void setUp() {}

  @Test
  public void compileSettings() {
    final Authentication.Provided providedAuth =
        Authentication.createProvided().withVerifierPredicate(c -> true);

    final Authentication.None noAuth = Authentication.createNone();

    final SourceSettings settings = SourceSettings.create(clientId);

    settings.withAuthentication(providedAuth);
    settings.withAuthentication(noAuth);
  }

  @Test
  public void compileSource() {
    // #source
    final SourceSettings settings = SourceSettings.create(clientId);

    final Source<ReferenceReadResult, CompletionStage<Done>> source = Reference.source(settings);
    // #source
  }

  @Test
  public void compileFlow() {
    // #flow
    final Flow<ReferenceWriteMessage, ReferenceWriteResult, NotUsed> flow = Reference.flow();
    // #flow

    final Executor ex = Executors.newCachedThreadPool();
    final Flow<ReferenceWriteMessage, ReferenceWriteResult, NotUsed> flow2 =
        Reference.flowAsyncMapped(ex);
  }

  @Test
  public void runSource() throws Exception {
    final Source<ReferenceReadResult, CompletionStage<Done>> source =
        Reference.source(SourceSettings.create(clientId));

    final CompletionStage<ReferenceReadResult> stage = source.runWith(Sink.head(), system);
    final ReferenceReadResult msg = stage.toCompletableFuture().get(5, TimeUnit.SECONDS);

    Assert.assertEquals(Collections.singletonList(ByteString.fromString("one")), msg.getData());

    final OptionalInt expected = OptionalInt.of(100);
    Assert.assertEquals(expected, msg.getBytesRead());

    Assert.assertEquals(Optional.empty(), msg.getBytesReadFailure());
  }

  @Test
  public void runFlow() throws Exception {
    final Flow<ReferenceWriteMessage, ReferenceWriteResult, NotUsed> flow = Reference.flow();

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

    final CompletionStage<List<ReferenceWriteResult>> stage =
        source.via(flow).runWith(Sink.seq(), system);
    final List<ReferenceWriteResult> result = stage.toCompletableFuture().get(5, TimeUnit.SECONDS);

    final List<ByteString> bytes =
        result.stream()
            .flatMap(m -> m.getMessage().getData().stream())
            .collect(Collectors.toList());

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

  @Test
  public void resolveResourceFromApplicationConfig() throws Exception {
    final List<ReferenceWriteResult> result =
        Source.single(
                ReferenceWriteMessage.create()
                    .withData(Collections.singletonList(ByteString.fromString("one"))))
            .via(Reference.flowWithResource())
            .runWith(Sink.seq(), system)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS);

    Assert.assertEquals(
        Collections.singletonList("one default msg"),
        result.stream()
            .flatMap(m -> m.getMessage().getData().stream())
            .map(ByteString::utf8String)
            .collect(Collectors.toList()));
  }

  @Test
  public void useResourceFromAttributes() throws Exception {
    final List<ReferenceWriteResult> result =
        Source.single(
                ReferenceWriteMessage.create()
                    .withData(Collections.singletonList(ByteString.fromString("one"))))
            .via(
                Reference.flowWithResource()
                    .withAttributes(
                        ReferenceAttributes.resource(
                            Resource.create(ResourceSettings.create("attributes msg")))))
            .runWith(Sink.seq(), system)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS);

    Assert.assertEquals(
        Collections.singletonList("one attributes msg"),
        result.stream()
            .flatMap(m -> m.getMessage().getData().stream())
            .map(ByteString::utf8String)
            .collect(Collectors.toList()));
  }

  /** Called after every test. */
  @After
  public void tearDown() {}

  /** Called after test suite. */
  @AfterClass
  public static void tearDownAfterClass() {
    TestKit.shutdownActorSystem(system);
  }
}
