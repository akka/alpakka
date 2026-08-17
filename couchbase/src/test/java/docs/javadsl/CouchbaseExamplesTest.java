/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package docs.javadsl;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import akka.Done;
import akka.actor.ActorSystem;
import akka.stream.Materializer;
import akka.stream.alpakka.couchbase.*;
// #deleteWithResult
import akka.stream.alpakka.couchbase.CouchbaseDeleteResult;
// #deleteWithResult
// #fromId #session #fromCluster
import akka.stream.alpakka.couchbase.CouchbaseDocument;
// #fromId #session #fromCluster
// #registry
import akka.stream.alpakka.couchbase.CouchbaseSessionRegistry;
// #registry
// #registry #session
import akka.stream.alpakka.couchbase.CouchbaseSessionSettings;
// #registry #session
// #upsertWithResult #replaceWithResult
import akka.stream.alpakka.couchbase.CouchbaseWriteFailure;
// #upsertWithResult #replaceWithResult
// #upsert #upsertWithResult #replace #replaceWithResult
import akka.stream.alpakka.couchbase.CouchbaseWriteResult;
// #upsert #upsertWithResult #replace #replaceWithResult
// #fromId #upsert #upsertWithResult #replace #replaceWithResult #delete #deleteWithResult
import akka.stream.alpakka.couchbase.javadsl.CouchbaseFlow;
// #fromId #upsert #upsertWithResult #replace #replaceWithResult #delete #deleteWithResult
// #registry #session #fromCluster
import akka.stream.alpakka.couchbase.javadsl.CouchbaseSession;
// #registry #session #fromCluster
// #statement
import akka.stream.alpakka.couchbase.javadsl.CouchbaseSource;
// #statement
import akka.stream.alpakka.couchbase.testing.CouchbaseSupportClass;
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
// #fromId #upsert #upsertWithResult #replace #replaceWithResult #delete #deleteWithResult
import akka.stream.javadsl.Sink;
// #fromId #upsert #upsertWithResult #replace #replaceWithResult #delete #deleteWithResult
// #fromId #upsert #upsertWithResult #replace #replaceWithResult #delete #deleteWithResult
import akka.stream.javadsl.Source;
// #fromId #upsert #upsertWithResult #replace #replaceWithResult #delete #deleteWithResult
import akka.stream.testkit.javadsl.StreamTestKit;
import com.couchbase.client.core.error.DocumentNotFoundException;
// #fromCluster
import com.couchbase.client.java.Cluster;
// #fromCluster
// #registry
import com.couchbase.client.java.env.ClusterEnvironment;
// #registry
// #statement
import com.couchbase.client.java.json.JsonObject;
// #statement
import com.couchbase.client.java.json.JsonValue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import org.junit.*;

public class CouchbaseExamplesTest {

  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  private static final CouchbaseSupportClass support = new CouchbaseSupportClass();
  private static final CouchbaseSessionSettings sessionSettings = support.sessionSettings();
  private static final String bucketName = support.bucketName();
  private static final String queryBucketName = support.bucketName();
  private static ActorSystem actorSystem;
  private static CouchbaseDocument<String> sampleData;
  private static List<CouchbaseDocument<String>> sampleSequence;

  @BeforeClass
  public static void beforeAll() {
    support.beforeAll();
    actorSystem = support.actorSystem();
    sampleData = support.sampleData();
    sampleSequence = support.sampleJavaList();
  }

  @AfterClass
  public static void afterAll() {
    support.afterAll();
  }

  @After
  public void checkForStageLeaks() {
    StreamTestKit.assertAllStagesStopped(Materializer.matFromSystem(actorSystem));
  }

  @Test
  public void registry() throws Exception {
    // #registry

    CouchbaseSessionRegistry registry = CouchbaseSessionRegistry.get(actorSystem);

    // If connecting to more than one Couchbase cluster, the environment should be shared
    ClusterEnvironment environment = ClusterEnvironment.create();
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
          CompletionStage<CouchbaseDocument<byte[]>> documentCompletionStage =
              session.collection(support.scopeName(), support.collectionName()).getBytes(id);
          documentCompletionStage
              .exceptionally(
                  ex -> {
                    ex.printStackTrace();
                    return null;
                  })
              .thenAccept(
                  doc -> {
                    if (doc != null) {
                      System.out.println(doc);
                    }
                  });
        });
    // #session
  }

  @Test
  public void sessionFromBucket() {
    // #fromCluster
    Cluster cluster = Cluster.connect("localhost", "Administrator", "password");
    CouchbaseSession.create(cluster.async(), bucketName)
        .thenAccept(
            session -> {
              actorSystem.registerOnTermination(
                  () -> {
                    session.close();
                    cluster.close();
                  });

              String id = "First";
              CompletionStage<CouchbaseDocument<byte[]>> documentCompletionStage =
                  session.collection(support.scopeName(), support.collectionName()).getBytes(id);
              documentCompletionStage.thenAccept(
                  opt -> {
                    if (opt != null) {
                      System.out.println(opt.getDocument());
                    } else {
                      System.out.println("Document " + id + " wasn't found");
                    }
                  });
              // #fromCluster
            });
  }

  @Test
  public void query() throws Exception {
    support.upsertSampleData(queryBucketName, support.scopeName(), support.collectionName());
    // #statement

    CompletionStage<List<JsonObject>> resultCompletionStage =
        CouchbaseSource.fromQuery(
                sessionSettings,
                bucketName,
                "SELECT * FROM `"
                    + support.bucketName()
                    + "`.`"
                    + support.scopeName()
                    + "`.`"
                    + support.collectionName()
                    + "` LIMIT 10")
            .runWith(Sink.seq(), actorSystem);
    // #statement
    List<JsonObject> jsonObjects =
        resultCompletionStage.toCompletableFuture().get(3, TimeUnit.SECONDS);
    assertEquals(4, jsonObjects.size());
  }

  @Test
  public void fromId() throws Exception {
    support.upsertSampleData(queryBucketName, support.scopeName(), support.collectionName());
    // #fromId
    List<String> ids = Arrays.asList("First", "Second", "Third", "Fourth");
    List<String> idsJson = Arrays.asList("FirstJson", "SecondJson", "ThirdJson", "FourthJson");

    CompletionStage<List<CouchbaseDocument<byte[]>>> result =
        Source.from(ids)
            .via(
                CouchbaseFlow.bytesFromId(
                    sessionSettings,
                    queryBucketName,
                    support.scopeName(),
                    support.collectionName()))
            .runWith(Sink.seq(), actorSystem);

    // #fromId

    List<CouchbaseDocument<byte[]>> docs = result.toCompletableFuture().get(3, TimeUnit.SECONDS);
    assertEquals(4, docs.size());
  }

  public void fromIdJson() throws Exception {
    support.upsertSampleData(queryBucketName, support.scopeName(), support.collectionName());
    // #fromId
    List<String> idsJson = Arrays.asList("FirstJson", "SecondJson", "ThirdJson", "FourthJson");
    CompletionStage<List<CouchbaseDocument<JsonValue>>> jsonResult =
        Source.from(idsJson)
            .via(
                CouchbaseFlow.fromId(
                    sessionSettings,
                    queryBucketName,
                    support.scopeName(),
                    support.collectionName()))
            .runWith(Sink.seq(), actorSystem);
    // #fromId

    List<CouchbaseDocument<JsonValue>> jsonDocs =
        jsonResult.toCompletableFuture().get(3, TimeUnit.SECONDS);
    assertEquals(4, jsonDocs.size());
  }

  @Test
  public void upsert() throws Exception {

    CouchbaseDocument<String> obj = new CouchbaseDocument<>("First", "First");

    // #upsert
    CompletionStage<Done> jsonDocumentUpsert =
        Source.single(obj)
            .via(
                CouchbaseFlow.upsert(
                    sessionSettings, bucketName, support.scopeName(), support.collectionName()))
            .runWith(Sink.head(), actorSystem);
    // #upsert

    jsonDocumentUpsert.toCompletableFuture().get(3, TimeUnit.SECONDS);
  }

  @Test
  public void upsertWithResult() throws Exception {

    // #upsertWithResult
    CompletionStage<List<CouchbaseWriteResult>> upsertResults =
        Source.from(sampleSequence)
            .via(
                CouchbaseFlow.upsertWithResult(
                    sessionSettings, bucketName, support.scopeName(), support.collectionName()))
            .runWith(Sink.seq(), actorSystem);

    List<CouchbaseWriteResult> writeResults =
        upsertResults.toCompletableFuture().get(3, TimeUnit.SECONDS);
    List<CouchbaseWriteFailure> failedDocs =
        writeResults.stream()
            .filter(CouchbaseWriteResult::isFailure)
            .map(CouchbaseWriteFailure.class::cast)
            .collect(Collectors.toUnmodifiableList());
    // #upsertWithResult

    assertThat(writeResults.size(), is(sampleSequence.size()));
    assertTrue("unexpected failed writes", failedDocs.isEmpty());
  }

  @Test
  public void replace() throws Exception {

    support.upsertSampleData(bucketName, support.scopeName(), support.collectionName());

    CouchbaseDocument<String> obj = new CouchbaseDocument<>("First", "FirstReplace");

    // #replace
    CompletionStage<CouchbaseWriteResult> jsonDocumentReplace =
        Source.single(obj)
            .via(
                CouchbaseFlow.replaceWithResult(
                    sessionSettings, bucketName, support.scopeName(), support.collectionName()))
            .runWith(Sink.head(), actorSystem);
    // #replace

    CouchbaseWriteResult result =
        jsonDocumentReplace.toCompletableFuture().get(3, TimeUnit.SECONDS);

    assertTrue(result.isSuccess());
  }

  @Test(expected = DocumentNotFoundException.class)
  public void replaceFailsWhenDocumentDoesntExists() throws Throwable {

    support.cleanAllInCollection(bucketName, support.scopeName(), support.collectionName());

    CouchbaseDocument<String> obj = new CouchbaseDocument<>("First", "FirstReplace");

    // #replace
    CompletionStage<Done> jsonDocumentReplace =
        Source.single(obj)
            .via(
                CouchbaseFlow.replace(
                    sessionSettings, bucketName, support.scopeName(), support.collectionName()))
            .runWith(Sink.head(), actorSystem);
    // #replace

    try {
      jsonDocumentReplace.toCompletableFuture().get(3, TimeUnit.SECONDS);
    } catch (ExecutionException ex) {
      throw ex.getCause();
    }
  }

  @Test
  public void replaceWithResult() throws Exception {

    support.upsertSampleData(bucketName, support.scopeName(), support.collectionName());

    List<CouchbaseDocument<String>> list = new ArrayList<>();
    list.add(new CouchbaseDocument<>("First", "FirstReplace"));
    list.add(new CouchbaseDocument<>("Second", "SecondReplace"));
    list.add(new CouchbaseDocument<>("Third", "ThirdReplace"));
    list.add(new CouchbaseDocument<>("NotExisting", "Nothing")); // should fail
    list.add(new CouchbaseDocument<>("Fourth", "FourthReplace"));

    // #replaceWithResult
    CompletionStage<List<CouchbaseWriteResult>> replaceResults =
        Source.from(list)
            .via(
                CouchbaseFlow.replaceWithResult(
                    sessionSettings, bucketName, support.scopeName(), support.collectionName()))
            .runWith(Sink.seq(), actorSystem);

    List<CouchbaseWriteResult> writeResults =
        replaceResults.toCompletableFuture().get(3, TimeUnit.SECONDS);
    List<CouchbaseWriteFailure> failedDocs =
        writeResults.stream()
            .filter(CouchbaseWriteResult::isFailure)
            .map(CouchbaseWriteFailure.class::cast)
            .collect(Collectors.toUnmodifiableList());
    // #replaceWithResult

    assertThat(writeResults.size(), is(list.size()));
    assertThat(failedDocs.size(), is(1));
  }

  @Test
  public void delete() throws Exception {
    // #delete
    CompletionStage<String> result =
        Source.single(sampleData.getId())
            .via(
                CouchbaseFlow.delete(
                    sessionSettings, bucketName, support.scopeName(), support.collectionName()))
            .runWith(Sink.head(), actorSystem);
    // #delete

    String id = result.toCompletableFuture().get(3, TimeUnit.SECONDS);

    assertSame(sampleData.getId(), id);
  }

  @Test
  public void deleteWithResult() throws Exception {
    // #deleteWithResult
    CompletionStage<CouchbaseDeleteResult> result =
        Source.single("non-existent")
            .via(
                CouchbaseFlow.deleteWithResult(
                    sessionSettings, bucketName, support.scopeName(), support.collectionName()))
            .runWith(Sink.head(), actorSystem);
    CouchbaseDeleteResult deleteResult = result.toCompletableFuture().get(3, TimeUnit.SECONDS);
    // #deleteWithResult
    assertTrue(deleteResult.isFailure());
  }
}
