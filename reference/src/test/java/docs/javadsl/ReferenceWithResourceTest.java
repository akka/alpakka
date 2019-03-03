/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.reference.ReferenceWriteMessage;
import akka.stream.alpakka.reference.ReferenceWriteResult;
import akka.stream.alpakka.reference.Resource;
import akka.stream.alpakka.reference.ResourceExt;
import akka.stream.alpakka.reference.javadsl.ReferenceWithExternalResource;
import akka.stream.alpakka.reference.javadsl.ReferenceWithResource;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;
import org.junit.*;

/** Append "Test" to every Java test suite. */
public class ReferenceWithResourceTest {

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
  public void useGlobalResource() {
    final Flow<ReferenceWriteMessage, ReferenceWriteResult, NotUsed> flow =
        ReferenceWithResource.flow(sys);

    Source.single(ReferenceWriteMessage.create()).via(flow).to(Sink.seq()).run(mat);
  }

  @Test
  public void useExternalResource() {
    final Resource resource = ResourceExt.get(sys).resource();
    final Flow<ReferenceWriteMessage, ReferenceWriteResult, NotUsed> flow =
        ReferenceWithExternalResource.flow(resource);

    Source.single(ReferenceWriteMessage.create()).via(flow).to(Sink.seq()).run(mat);
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
