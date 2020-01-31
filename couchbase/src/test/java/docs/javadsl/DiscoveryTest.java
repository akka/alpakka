/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
// #registry
import akka.stream.alpakka.couchbase.CouchbaseSessionRegistry;
import akka.stream.alpakka.couchbase.CouchbaseSessionSettings;
import akka.stream.alpakka.couchbase.javadsl.DiscoverySupport;
import akka.stream.alpakka.couchbase.javadsl.CouchbaseSession;
// #registry
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import akka.testkit.javadsl.TestKit;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class DiscoveryTest {

  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  private static ActorSystem actorSystem;
  private static Materializer materializer;
  private static final String bucketName = "akka";

  @BeforeClass
  public static void beforeAll() {
    Config config = ConfigFactory.parseResources("discovery.conf");
    actorSystem = ActorSystem.create("DiscoveryTest", config);
    materializer = ActorMaterializer.create(actorSystem);
  }

  @AfterClass
  public static void afterAll() {
    TestKit.shutdownActorSystem(actorSystem);
  }

  @Test
  public void configDiscovery() throws Exception {
    // #registry

    CouchbaseSessionRegistry registry = CouchbaseSessionRegistry.get(actorSystem);

    CouchbaseSessionSettings sessionSettings =
        CouchbaseSessionSettings.create(actorSystem)
            .withEnrichAsyncCs(DiscoverySupport.getNodes(actorSystem));
    CompletionStage<CouchbaseSession> session = registry.getSessionFor(sessionSettings, bucketName);
    // #registry
    try {
      CouchbaseSession couchbaseSession = session.toCompletableFuture().get(5, TimeUnit.SECONDS);
    } catch (java.util.concurrent.ExecutionException e) {
      assertThat(
          e.getCause(),
          is(instanceOf(com.couchbase.client.core.config.ConfigurationException.class)));
    }
  }
}
