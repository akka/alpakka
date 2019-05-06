/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.actor.ActorSystem;
import akka.stream.Materializer;
// #deleteWithResult
import akka.stream.alpakka.couchbase.CouchbaseDeleteResult;
// #deleteWithResult
// #upsertDocWithResult
import akka.stream.alpakka.couchbase.CouchbaseWriteFailure;
import akka.stream.alpakka.couchbase.CouchbaseWriteResult;
// #upsertDocWithResult
import akka.stream.alpakka.couchbase.CouchbaseWriteSettings;
import akka.stream.alpakka.couchbase.javadsl.CouchbaseFlow;
import akka.stream.alpakka.couchbase.javadsl.CouchbaseSource;
import akka.stream.alpakka.couchbase.testing.CouchbaseSupportClass;
import akka.stream.alpakka.couchbase.testing.TestObject;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.testkit.javadsl.StreamTestKit;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.StringDocument;
import com.couchbase.client.java.document.json.JsonObject;
// #registry
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
// #registry
// #n1ql
import com.couchbase.client.java.query.N1qlParams;
import com.couchbase.client.java.query.N1qlQuery;
// #n1ql
import com.couchbase.client.java.query.SimpleN1qlQuery;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
// #registry
import akka.stream.alpakka.couchbase.CouchbaseSessionRegistry;
// #session
import akka.stream.alpakka.couchbase.CouchbaseSessionSettings;
import akka.stream.alpakka.couchbase.javadsl.CouchbaseSession;
// #session #registry
import java.util.stream.Collectors;
// #sessionFromBucket
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.auth.PasswordAuthenticator;
// #sessionFromBucket
// #statement
import static com.couchbase.client.java.query.Select.select;
import static com.couchbase.client.java.query.dsl.Expression.*;
// #statement

import scala.concurrent.duration.FiniteDuration;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

public class CouchbaseExamplesTest {

  private static final CouchbaseSupportClass support = new CouchbaseSupportClass();
  private static final CouchbaseSessionSettings sessionSettings = support.sessionSettings();
  private static final String bucketName = support.bucketName();
  private static final String queryBucketName = support.queryBucketName();
  private static ActorSystem actorSystem;
  private static Materializer materializer;
  private static TestObject sampleData;
  private static List<TestObject> sampleSequence;

  @BeforeClass
  public static void beforeAll() {
    support.beforeAll();
    actorSystem = support.actorSystem();
    materializer = support.mat();
    sampleData = support.sampleData();
    sampleSequence = support.sampleJavaList();
  }

  @AfterClass
  public static void afterAll() {
    support.afterAll();
  }

  @After
  public void checkForStageLeaks() {
    StreamTestKit.assertAllStagesStopped(materializer);
  }

  @Test
  public void registry() throws Exception {
    // #registry

    CouchbaseSessionRegistry registry = CouchbaseSessionRegistry.get(actorSystem);

    // If connecting to more than one Couchbase cluster, the environment should be shared
    CouchbaseEnvironment environment = DefaultCouchbaseEnvironment.create();
    actorSystem.registerOnTermination(() -> environment.shutdown());

    CouchbaseSessionSettings sessionSettings =
        CouchbaseSessionSettings.create(actorSystem).withEnvironment(environment);
    CompletionStage<CouchbaseSession> sessionCompletionStage =
        registry.getSessionFor(sessionSettings, bucketName);
    // #registry
    CouchbaseSession session =
        sessionCompletionStage.toCompletableFuture().get(3, TimeUnit.SECONDS);
    assertNotNull(session);
  }

  @Test
  public void session() {
    // #session

    Executor executor = Executors.newSingleThreadExecutor();
    CouchbaseSessionSettings sessionSettings = CouchbaseSessionSettings.create(actorSystem);
    CompletionStage<CouchbaseSession> sessionCompletionStage =
        CouchbaseSession.create(sessionSettings, bucketName, executor);
    actorSystem.registerOnTermination(
        () -> sessionCompletionStage.thenAccept(CouchbaseSession::close));

    sessionCompletionStage.thenAccept(
        session -> {
          String id = "myId";
          CompletionStage<Optional<JsonDocument>> documentCompletionStage = session.get(id);
          documentCompletionStage.thenAccept(
              opt -> {
                if (opt.isPresent()) {
                  System.out.println(opt.get());
                } else {
                  System.out.println("Document " + id + " wasn't found");
                }
              });
        });
    // #session
  }

  @Test
  public void sessionFromBucket() {
    // #sessionFromBucket

    CouchbaseCluster cluster = CouchbaseCluster.create("localhost");
    cluster.authenticate(new PasswordAuthenticator("Administrator", "password"));
    Bucket bucket = cluster.openBucket("akka");
    CouchbaseSession session = CouchbaseSession.create(bucket);
    actorSystem.registerOnTermination(
        () -> {
          session.close();
          bucket.close();
        });

    String id = "First";
    CompletionStage<Optional<JsonDocument>> documentCompletionStage = session.get(id);
    documentCompletionStage.thenAccept(
        opt -> {
          if (opt.isPresent()) {
            System.out.println(opt.get());
          } else {
            System.out.println("Document " + id + " wasn't found");
          }
        });
    // #sessionFromBucket
  }

  @Test
  public void statement() throws Exception {
    support.upsertSampleData();
    // #statement

    CompletionStage<List<JsonObject>> resultCompletionStage =
        CouchbaseSource.fromStatement(
                sessionSettings, select("*").from(i(queryBucketName)).limit(10), bucketName)
            .runWith(Sink.seq(), materializer);
    // #statement
    List<JsonObject> jsonObjects =
        resultCompletionStage.toCompletableFuture().get(3, TimeUnit.SECONDS);
    assertEquals(4, jsonObjects.size());
  }

  @Test
  public void n1ql() throws Exception {
    support.upsertSampleData();
    // #n1ql

    N1qlParams params = N1qlParams.build().adhoc(false);
    SimpleN1qlQuery query = N1qlQuery.simple("select count(*) from " + queryBucketName, params);

    CompletionStage<JsonObject> resultCompletionStage =
        CouchbaseSource.fromN1qlQuery(sessionSettings, query, bucketName)
            .runWith(Sink.head(), materializer);
    // #n1ql
    JsonObject jsonObjects = resultCompletionStage.toCompletableFuture().get(3, TimeUnit.SECONDS);
    assertEquals(4, jsonObjects.getInt("$1").intValue());
  }

  @Test
  public void settings() {
    // #write-settings
    CouchbaseWriteSettings writeSettings =
        CouchbaseWriteSettings.create()
            .withParallelism(3)
            .withPersistTo(PersistTo.FOUR)
            .withReplicateTo(ReplicateTo.THREE)
            .withTimeout(Duration.ofSeconds(5));
    // #write-settings

    assertEquals(writeSettings.timeout(), FiniteDuration.apply(5, TimeUnit.SECONDS));
  }

  @Test
  public void fromId() throws Exception {
    support.upsertSampleData();
    // #fromId
    List<String> ids = Arrays.asList("First", "Second", "Third", "Fourth");

    CompletionStage<List<JsonDocument>> result =
        Source.from(ids)
            .via(CouchbaseFlow.fromId(sessionSettings, queryBucketName))
            .runWith(Sink.seq(), materializer);
    // #fromId

    List<JsonDocument> jsonObjects = result.toCompletableFuture().get(3, TimeUnit.SECONDS);
    assertEquals(4, jsonObjects.size());
  }

  @Test
  public void upsert() {
    // #upsert
    TestObject obj = new TestObject("First", "First");

    CouchbaseWriteSettings writeSettings = CouchbaseWriteSettings.create();

    CompletionStage<Done> jsonDocumentUpsert =
        Source.single(obj)
            .map(support::toJsonDocument)
            .via(CouchbaseFlow.upsert(sessionSettings, writeSettings, bucketName))
            .runWith(Sink.ignore(), materializer);
    // #upsert
  }

  @Test
  public void upsertDoc() {
    CouchbaseWriteSettings writeSettings = CouchbaseWriteSettings.create();
    // #upsert

    CompletionStage<Done> stringDocumentUpsert =
        Source.single(sampleData)
            .map(support::toStringDocument)
            .via(CouchbaseFlow.upsertDoc(sessionSettings, writeSettings, bucketName))
            .runWith(Sink.ignore(), materializer);
    // #upsert
  }

  @Test
  public void upsertDocWitResult() throws Exception {
    CouchbaseWriteSettings writeSettings = CouchbaseWriteSettings.create();
    // #upsertDocWithResult

    CompletionStage<List<CouchbaseWriteResult<StringDocument>>> upsertResults =
        Source.from(sampleSequence)
            .map(support::toStringDocument)
            .via(CouchbaseFlow.upsertDocWithResult(sessionSettings, writeSettings, bucketName))
            .runWith(Sink.seq(), materializer);

    List<CouchbaseWriteResult<StringDocument>> writeResults =
        upsertResults.toCompletableFuture().get(3, TimeUnit.SECONDS);
    List<CouchbaseWriteFailure<StringDocument>> failedDocs =
        writeResults.stream()
            .filter(CouchbaseWriteResult::isFailure)
            .map(res -> (CouchbaseWriteFailure<StringDocument>) res)
            .collect(Collectors.toList());
    // #upsertDocWithResult
    assertThat(writeResults.size(), is(sampleSequence.size()));
    assertTrue("unexpected failed writes", failedDocs.isEmpty());
  }

  @Test
  public void delete() {
    CouchbaseWriteSettings writeSettings = CouchbaseWriteSettings.create();
    // #delete
    CompletionStage<Done> result =
        Source.single(sampleData.id())
            .via(CouchbaseFlow.delete(sessionSettings, writeSettings, bucketName))
            .runWith(Sink.ignore(), materializer);
    // #delete
  }

  @Test
  public void deleteWithResult() throws Exception {
    CouchbaseWriteSettings writeSettings = CouchbaseWriteSettings.create();
    // #deleteWithResult
    CompletionStage<CouchbaseDeleteResult> result =
        Source.single("non-existent")
            .via(CouchbaseFlow.deleteWithResult(sessionSettings, writeSettings, bucketName))
            .runWith(Sink.head(), materializer);
    // #deleteWithResult
    CouchbaseDeleteResult deleteResult = result.toCompletableFuture().get(3, TimeUnit.SECONDS);
    assertTrue(deleteResult.isFailure());
  }
}
