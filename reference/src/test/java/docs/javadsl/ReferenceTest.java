/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

/*
 * Start package with 'docs' prefix when testing APIs as a user.
 * This prevents any visibility issues that may be hidden.
 */
package docs.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;
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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * Append "Test" to every Java test suite.
 */
public class ReferenceTest {

  static ActorSystem sys;
  static Materializer mat;

  /**
   * Called before test suite.
   */
  @BeforeClass
  public static void setUpBeforeClass() {
    sys = ActorSystem.create("ReferenceTest");
    mat = ActorMaterializer.create(sys);
  }

  /**
   * Called before every test.
   */
  @Before
  public void setUp() {

  }

  @Test
  public void settingsCompilationTest() {
    final Authentication.Provided providedAuth =
      Authentication.createProvided().withVerifierPredicate(c -> true);

    final Authentication.None noAuth =
      Authentication.createNone();

    final SourceSettings settings = SourceSettings.create();

    settings.withAuthentication(providedAuth);
    settings.withAuthentication(noAuth);
  }

  @Test
  public void sourceCompilationTest() {
    // #source
    final SourceSettings settings = SourceSettings.create();

    final Source<ReferenceReadMessage, CompletionStage<NotUsed>> source =
      Reference.source(settings);
    // #source
  }

  @Test
  public void flowCompilationTest() {
    // #flow
    final Flow<ReferenceWriteMessage, ReferenceWriteMessage, NotUsed> flow =
      Reference.flow();
    // #flow

    final Executor ex = Executors.newCachedThreadPool();
    final Flow<ReferenceWriteMessage, ReferenceWriteMessage, NotUsed> flow2 =
      Reference.flowAsyncMapped(ex);
  }

  @Test
  public void testSource() throws Exception {
    final Source<ReferenceReadMessage, CompletionStage<NotUsed>> source =
      Reference.source(SourceSettings.create());

    final CompletionStage<ReferenceReadMessage> stage = source.runWith(Sink.head(), mat);
    final ReferenceReadMessage msg = stage.toCompletableFuture().get();

    Assert.assertEquals(
      msg.getData(),
      Arrays.asList(ByteString.fromString("one"))
    );
  }

  @Test
  public void testFlow() throws Exception {
    final Flow<ReferenceWriteMessage, ReferenceWriteMessage, NotUsed> flow =
      Reference.flow();

    final Source<ReferenceWriteMessage, NotUsed> source = Source.from(Arrays.asList(
      ReferenceWriteMessage.create().withData(
        ByteString.fromString("one")
      ),
      ReferenceWriteMessage.create().withData(
        ByteString.fromString("two"),
        ByteString.fromString("three"),
        ByteString.fromString("four")
      )
    ));

    final CompletionStage<List<ReferenceWriteMessage>> stage = source.via(flow).runWith(Sink.seq(), mat);
    final List<ByteString> result =
      stage.toCompletableFuture().get().stream()
        .flatMap(m -> m.getData().stream())
        .collect(Collectors.toList());

    Assert.assertEquals(
      result,
      Arrays.asList(
        ByteString.fromString("one"),
        ByteString.fromString("two"),
        ByteString.fromString("three"),
        ByteString.fromString("four")
      )
    );
  }

  /**
   * Called after every test.
   */
  @After
  public void tearDown() {

  }

  /**
   * Called after test suite.
   */
  @AfterClass
  public static void tearDownAfterClass() {
    TestKit.shutdownActorSystem(sys);
  }

}