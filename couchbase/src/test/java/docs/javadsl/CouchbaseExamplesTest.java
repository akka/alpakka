/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.stream.Materializer;
import akka.stream.alpakka.couchbase.CouchbaseSessionSettings;
import akka.stream.alpakka.couchbase.CouchbaseWriteSettings;
import akka.stream.alpakka.couchbase.javadsl.CouchbaseFlow;
import akka.stream.alpakka.couchbase.javadsl.CouchbaseSink;
import akka.stream.alpakka.couchbase.javadsl.CouchbaseSource;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.AsyncN1qlQueryRow;
import com.couchbase.client.java.query.N1qlParams;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.SimpleN1qlQuery;
import docs.scaladsl.CouchbaseSupportClass;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;

import static org.junit.Assert.*;

public class CouchbaseExamplesTest {

  private static final CouchbaseSupportClass support = new CouchbaseSupportClass();
  private static final CouchbaseSessionSettings sessionSettings = support.sessionSettings();
  private static final String bucketName = support.bucketName();
  private static Materializer materializer;

  @BeforeClass
  public static void beforeAll() {
    support.beforeAll();
    materializer = support.mat();
  }

  @AfterClass
  public static void afterAll() {
    support.afterAll();
  }

  @Test
  public void settings() {
    // #write-settings
    CouchbaseWriteSettings defaultCouchbaseWriteSettings = CouchbaseWriteSettings.create();

    CouchbaseWriteSettings asyncCouchbaseWriteSettings =
        CouchbaseWriteSettings.create().withParallelism(3);

    CouchbaseWriteSettings couchbaseWriteSettings =
        CouchbaseWriteSettings.create()
            .withParallelism(3)
            .withPersistTo(PersistTo.MASTER)
            .withPersistTo(PersistTo.FOUR);
    // #write-settings

    assertEquals(defaultCouchbaseWriteSettings, CouchbaseWriteSettings.create());
  }

  @Test
  public void fromId() {
    // #init-sourceSingle
    String id = "First";

    Source<JsonDocument, NotUsed> source = CouchbaseSource.fromId(sessionSettings, id, bucketName);

    CompletionStage<JsonDocument> result = source.runWith(Sink.head(), materializer);
    // #init-sourceSingle
  }

  @Test
  public void multipleFromId() {
    // #init-sourceBulk
    List<String> ids = Arrays.asList("One", "Two", "Three");

    Flow<String, JsonDocument, NotUsed> flow = CouchbaseFlow.fromId(sessionSettings, bucketName);

    CompletionStage<List<JsonDocument>> result =
        Source.from(ids).via(flow).runWith(Sink.seq(), materializer);
    // #init-sourceBulk
  }

  @Test
  public void n1qlQuery() {
    // #init-sourcen1ql
    N1qlParams params = N1qlParams.build().adhoc(false);
    SimpleN1qlQuery query = N1qlQuery.simple("select count(*) from akkaquery", params);

    Source<JsonObject, NotUsed> source =
        CouchbaseSource.fromN1qlQuery(sessionSettings, query, bucketName);

    CompletionStage<List<JsonObject>> result = source.runWith(Sink.seq(), materializer);
    // #init-sourcen1ql
  }

  @Test
  public void upsert() {
    // #upsertSingle

    CouchbaseWriteSettings writeSettings = CouchbaseWriteSettings.create();

    JsonDocument document =
        JsonDocument.create("id1", JsonObject.create().put("value", "some Value"));

    Sink<JsonDocument, CompletionStage<Done>> sink =
        CouchbaseSink.upsert(sessionSettings, writeSettings, bucketName);

    CompletionStage<Done> result = Source.single(document).runWith(sink, materializer);
    // #upsertSingle
  }

  @Test
  public void delete() {
    // #delete-single-sink

    CouchbaseWriteSettings writeSettings = CouchbaseWriteSettings.create();

    String id = "id1";

    Sink<String, CompletionStage<Done>> sink =
        CouchbaseSink.delete(sessionSettings, writeSettings, bucketName);

    CompletionStage<Done> result = Source.single(id).runWith(sink, materializer);
    // #delete-single-sink

  }
}
