/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package docs.javadsl;

import akka.actor.ActorSystem;
import akka.http.scaladsl.server.util.Tuple;
import akka.stream.Materializer;
// #deleteWithResult
import akka.stream.alpakka.couchbase.CouchbaseDeleteResult;
// #deleteWithResult
// #upsertDocWithResult
import akka.stream.alpakka.couchbase.CouchbaseWriteFailure;
import akka.stream.alpakka.couchbase.CouchbaseWriteResult;
// #upsertDocWithResult
import akka.stream.alpakka.couchbase.javadsl.CouchbaseFlow;
import akka.stream.alpakka.couchbase.javadsl.CouchbaseSource;
import akka.stream.alpakka.couchbase.testing.CouchbaseSupportClass;
import akka.stream.alpakka.couchbase.testing.TestObject;
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.testkit.javadsl.StreamTestKit;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.env.ClusterEnvironment;

import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.json.JsonValue;
import org.junit.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
// #registry
import akka.stream.alpakka.couchbase.CouchbaseSessionRegistry;
// #session
import akka.stream.alpakka.couchbase.CouchbaseSessionSettings;
import akka.stream.alpakka.couchbase.javadsl.CouchbaseSession;
// #session
// #registry
import java.util.stream.Collectors;
// #sessionFromBucket
import com.couchbase.client.java.Bucket;
// #sessionFromBucket

import scala.Tuple2;
import scala.concurrent.duration.FiniteDuration;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class CouchbaseExamplesTest {

  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  private static final CouchbaseSupportClass support = new CouchbaseSupportClass();
  private static final CouchbaseSessionSettings sessionSettings = support.sessionSettings();
  private static final String bucketName = support.bucketName();
  private static final String queryBucketName = support.bucketName();
  private static ActorSystem actorSystem;
  private static Tuple2<String, String> sampleData;
  private static List<Tuple2<String, String>> sampleSequence;

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
          CompletionStage<Tuple2<String, byte[]>> documentCompletionStage = session.collection(support.scopeName(), support.collectionName()).getBytes(id);
          documentCompletionStage.exceptionally(ex -> {
            ex.printStackTrace();
            return null;
          }).thenAccept(doc -> {
            if (doc != null) {
              System.out.println(doc);
            }
          });
        });
    // #session
  }

  @Test
  public void sessionFromBucket() {
    // #sessionFromBucket

    Cluster cluster = Cluster.connect("localhost", "Administrator", "password");
    CouchbaseSession.create(cluster.async(), bucketName).thenAccept(session -> {
      actorSystem.registerOnTermination(
              () -> {
                session.close();
                cluster.close();
              });

      String id = "First";
      CompletionStage<Tuple2<String, byte[]>> documentCompletionStage = session.collection(support.scopeName(), support.collectionName()).getBytes(id);
      documentCompletionStage.thenAccept(
              opt -> {
                if (opt != null) {
                  System.out.println(opt._2());
                } else {
                  System.out.println("Document " + id + " wasn't found");
                }
              });
      // #sessionFromBucket
    });

  }

  @Test
  public void query() throws Exception {
    support.upsertSampleData(queryBucketName, support.scopeName(), support.collectionName());
    // #statement

    CompletionStage<List<JsonObject>> resultCompletionStage =
        CouchbaseSource.fromQuery(
                sessionSettings, bucketName, "SELECT * FROM `" + support.bucketName() + "`.`" + support.scopeName() + "`.`" + support.collectionName() + "` LIMIT 10")
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

    CompletionStage<List<Tuple2<String, byte[]>>> result =
        Source.from(ids)
            .via(CouchbaseFlow.bytesFromId(sessionSettings, queryBucketName, support.scopeName(), support.collectionName()))
            .runWith(Sink.seq(), actorSystem);
    // #fromId

    List<Tuple2<String, byte[]>> docs = result.toCompletableFuture().get(3, TimeUnit.SECONDS);
    assertEquals(4, docs.size());
  }

  @Test
  public void upsert() throws Exception {

    Tuple2<String, String> obj = new Tuple2<>("First", "First");

    // #upsert
    CompletionStage<Tuple2<String, String>> jsonDocumentUpsert =
        Source.single(obj)
            .via(CouchbaseFlow.upsert(sessionSettings, bucketName, support.scopeName(), support.collectionName()))
            .runWith(Sink.head(), actorSystem);
    // #upsert

    Tuple2<String, String> document = jsonDocumentUpsert.toCompletableFuture().get(3, TimeUnit.SECONDS);

    assert (document._2().equals("First"));
  }

  @Test
  public void upsertWithResult() throws Exception {

    // #upsertDocWithResult
    CompletionStage<List<CouchbaseWriteResult<String>>> upsertResults =
        Source.from(sampleSequence)
            .via(CouchbaseFlow.upsertWithResult(sessionSettings, bucketName, support.scopeName(), support.collectionName()))
            .runWith(Sink.seq(), actorSystem);

    List<CouchbaseWriteResult<String>> writeResults =
        upsertResults.toCompletableFuture().get(3, TimeUnit.SECONDS);
    List<CouchbaseWriteFailure<String>> failedDocs =
        writeResults.stream()
            .filter(CouchbaseWriteResult::isFailure)
            .map(res -> (CouchbaseWriteFailure<String>) res)
            .toList();
    // #upsertDocWithResult

    assertThat(writeResults.size(), is(sampleSequence.size()));
    assertTrue("unexpected failed writes", failedDocs.isEmpty());
  }

  @Test
  public void replace() throws Exception {

    support.upsertSampleData(bucketName, support.scopeName(), support.collectionName());

    Tuple2<String, String> obj = new Tuple2<>("First", "FirstReplace");

    // #replace
    CompletionStage<Tuple2<String, String>> jsonDocumentReplace =
        Source.single(obj)
            .via(CouchbaseFlow.replace(sessionSettings, bucketName, support.scopeName(), support.collectionName()))
            .runWith(Sink.head(), actorSystem);
    // #replace

    Tuple2<String, String> document = jsonDocumentReplace.toCompletableFuture().get(3, TimeUnit.SECONDS);

    assert (document._2().equals("FirstReplace"));
  }

  @Test(expected = DocumentNotFoundException.class)
  public void replaceFailsWhenDocumentDoesntExists() throws Throwable {

    support.cleanAllInCollection(bucketName, support.scopeName(), support.collectionName());

    Tuple2<String, String> obj = new Tuple2<>("First", "FirstReplace");


    // #replace
    CompletionStage<Tuple2<String, String>> jsonDocumentReplace =
        Source.single(obj)
            .via(CouchbaseFlow.replace(sessionSettings, bucketName, support.scopeName(), support.collectionName()))
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

    List<Tuple2<String, String>> list = new ArrayList<>();
    list.add(new Tuple2<>("First", "FirstReplace"));
    list.add(new Tuple2<>("Second", "SecondReplace"));
    list.add(new Tuple2<>("Third", "ThirdReplace"));
    list.add(new Tuple2<>("NotExisting", "Nothing")); // should fail
    list.add(new Tuple2<>("Fourth", "FourthReplace"));

    // #replaceDocWithResult
    CompletionStage<List<CouchbaseWriteResult<String>>> replaceResults =
        Source.from(list)
            .via(CouchbaseFlow.replaceWithResult(sessionSettings, bucketName, support.scopeName(), support.collectionName()))
            .runWith(Sink.seq(), actorSystem);

    List<CouchbaseWriteResult<String>> writeResults =
        replaceResults.toCompletableFuture().get(3, TimeUnit.SECONDS);
    List<CouchbaseWriteFailure<String>> failedDocs =
        writeResults.stream()
            .filter(CouchbaseWriteResult::isFailure)
            .map(res -> (CouchbaseWriteFailure<String>) res)
            .toList();
    // #replaceDocWithResult

    assertThat(writeResults.size(), is(list.size()));
    assertThat(failedDocs.size(), is(1));
  }

  @Test
  public void delete() throws Exception {
    // #delete
    CompletionStage<String> result =
        Source.single(sampleData._1())
            .via(CouchbaseFlow.delete(sessionSettings, bucketName, support.scopeName(), support.collectionName()))
            .runWith(Sink.head(), actorSystem);
    // #delete

    String id = result.toCompletableFuture().get(3, TimeUnit.SECONDS);

    assertSame(sampleData._1(), id);
  }

  @Test
  public void deleteWithResult() throws Exception {
    // #deleteWithResult
    CompletionStage<CouchbaseDeleteResult> result =
        Source.single("non-existent")
            .via(CouchbaseFlow.deleteWithResult(sessionSettings, bucketName, support.scopeName(), support.collectionName()))
            .runWith(Sink.head(), actorSystem);
    // #deleteWithResult
    CouchbaseDeleteResult deleteResult = result.toCompletableFuture().get(3, TimeUnit.SECONDS);
    assertTrue(deleteResult.isFailure());
  }
}
