/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.alpakka.couchbase.CouchbaseWriteSettings;

// #init-sourceSingle
import akka.stream.alpakka.couchbase.javadsl.*;

import java.util.concurrent.CompletionStage;

// #init-sourceSingle

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.auth.PasswordAuthenticator;
// #init-sourcen1ql
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.AsyncN1qlQueryRow;
import com.couchbase.client.java.query.N1qlParams;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.SimpleN1qlQuery;

// #init-sourcen1ql

import java.util.Arrays;
import java.util.List;

// #upsertSingle
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.document.JsonDocument;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;

import scala.collection.Seq;

// #upsertSingle

// #upsertFlowSingle
import akka.stream.javadsl.Flow;
import akka.stream.alpakka.couchbase.javadsl.CouchbaseFlow;
import akka.stream.alpakka.couchbase.SingleOperationResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
// #upsertFlowSingle

// #by-single-id-flow
import akka.stream.javadsl.Flow;
import akka.stream.alpakka.couchbase.javadsl.CouchbaseFlow;
// #by-single-id-flow

// #delete-bulk-flow
import akka.stream.alpakka.couchbase.BulkOperationResult;

// #delete-bulk-flow

public class Examples {

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

  private void javagetSingleSnippet() {

    // #init-actor-system
    ActorSystem system = ActorSystem.create();
    ActorMaterializer materializer = ActorMaterializer.create(system);
    // #init-actor-system

    // #cluster-connect
    CouchbaseCluster cluster = CouchbaseCluster.create("[Cluster Address]");
    cluster.authenticate(new PasswordAuthenticator("[Some Username]", "[Some password]"));
    Bucket queryBucket = cluster.openBucket("[some bucket name]");
    // #cluster-connect

    // #init-sourceSingle
    String id = "First";

    Source<JsonDocument, NotUsed> source =
        CouchbaseSource.fromSingleId(id, queryBucket, JsonDocument.class);

    CompletionStage<JsonDocument> result = source.runWith(Sink.head(), materializer);
    // #init-sourceSingle
  }

  private void javaBulkSnippet() {

    ActorSystem system = ActorSystem.create();
    ActorMaterializer materializer = ActorMaterializer.create(system);

    CouchbaseCluster cluster = CouchbaseCluster.create("[Cluster Address]");
    cluster.authenticate(new PasswordAuthenticator("[Some Username]", "[Some password]"));
    Bucket queryBucket = cluster.openBucket("[some bucket name]");

    // #init-sourceBulk
    List<String> ids = Arrays.asList("One", "Two", "Three");

    Source<JsonDocument, NotUsed> source =
        CouchbaseSource.fromIdBulk(ids, queryBucket, JsonDocument.class);

    CompletionStage<List<JsonDocument>> result = source.runWith(Sink.seq(), materializer);
    // #init-sourceBulk
  }

  private void javaN1QLSnippet() {

    ActorSystem system = ActorSystem.create();
    ActorMaterializer materializer = ActorMaterializer.create(system);

    CouchbaseCluster cluster = CouchbaseCluster.create("[Cluster Address]");
    cluster.authenticate(new PasswordAuthenticator("[Some Username]", "[Some password]"));
    Bucket queryBucket = cluster.openBucket("[some bucket name]");

    // #init-sourcen1ql
    N1qlParams params = N1qlParams.build().adhoc(false);
    SimpleN1qlQuery query = N1qlQuery.simple("select count(*) from akkaquery", params);

    Source<AsyncN1qlQueryRow, NotUsed> source = CouchbaseSource.fromN1qlQuery(query, queryBucket);

    CompletionStage<List<AsyncN1qlQueryRow>> result = source.runWith(Sink.seq(), materializer);
    // #init-sourcen1ql
  }

  private void UpsertSingleSinkSnippet() {
    ActorSystem system = ActorSystem.create();
    ActorMaterializer materializer = ActorMaterializer.create(system);

    CouchbaseCluster cluster = CouchbaseCluster.create("[Cluster Address]");
    cluster.authenticate(new PasswordAuthenticator("[Some Username]", "[Some password]"));
    Bucket queryBucket = cluster.openBucket("[some bucket name]");

    // #upsertSingle

    CouchbaseWriteSettings defaultCouchbaseWriteSettings = CouchbaseWriteSettings.create();

    JsonDocument document =
        JsonDocument.create("id1", JsonObject.create().put("value", "some Value"));

    Sink<JsonDocument, CompletionStage<Done>> sink =
        CouchbaseSink.upsertSingle(defaultCouchbaseWriteSettings, queryBucket);

    CompletionStage<Done> result = Source.single(document).runWith(sink, materializer);

    // #upsertSingle

  }

  private void UpsertBulkSinkSnippet() {
    ActorSystem system = ActorSystem.create();
    ActorMaterializer materializer = ActorMaterializer.create(system);

    CouchbaseCluster cluster = CouchbaseCluster.create("[Cluster Address]");
    cluster.authenticate(new PasswordAuthenticator("[Some Username]", "[Some password]"));
    Bucket queryBucket = cluster.openBucket("[some bucket name]");

    // #upsertBulk

    CouchbaseWriteSettings defaultCouchbaseWriteSettings = CouchbaseWriteSettings.create();

    JsonDocument document =
        JsonDocument.create("id1", JsonObject.create().put("value", "some Value"));
    JsonDocument document2 =
        JsonDocument.create("id2", JsonObject.create().put("value", "some other Value"));

    List<JsonDocument> docs = Arrays.asList(document, document2);

    Sink<List<JsonDocument>, CompletionStage<Done>> sink =
        CouchbaseSink.upsertBulk(defaultCouchbaseWriteSettings, queryBucket);

    CompletionStage<Done> result =
        Source.fromIterator(docs::iterator).grouped(2).runWith(sink, materializer);

    // #upsertBulk
  }

  private void DeleteSingleSinkSnippet() {
    ActorSystem system = ActorSystem.create();
    ActorMaterializer materializer = ActorMaterializer.create(system);

    CouchbaseCluster cluster = CouchbaseCluster.create("[Cluster Address]");
    cluster.authenticate(new PasswordAuthenticator("[Some Username]", "[Some password]"));
    Bucket queryBucket = cluster.openBucket("[some bucket name]");

    // #delete-single-sink

    CouchbaseWriteSettings defaultCouchbaseWriteSettings = CouchbaseWriteSettings.create();

    String id = "id1";

    Sink<String, CompletionStage<Done>> sink =
        CouchbaseSink.deleteSingle(defaultCouchbaseWriteSettings, queryBucket);

    CompletionStage<Done> result = Source.single(id).runWith(sink, materializer);
    // #delete-single-sink
  }

  private void DeleteBulkSinkSnippet() {
    ActorSystem system = ActorSystem.create();
    ActorMaterializer materializer = ActorMaterializer.create(system);

    CouchbaseCluster cluster = CouchbaseCluster.create("[Cluster Address]");
    cluster.authenticate(new PasswordAuthenticator("[Some Username]", "[Some password]"));
    Bucket queryBucket = cluster.openBucket("[some bucket name]");

    // #delete-bulk-sink

    CouchbaseWriteSettings defaultCouchbaseWriteSettings = CouchbaseWriteSettings.create();

    String id = "id1";
    String id2 = "id2";
    List<String> ids = Arrays.asList(id, id2);

    Sink<List<String>, CompletionStage<Done>> sink =
        CouchbaseSink.deleteBulk(defaultCouchbaseWriteSettings, queryBucket);
    CompletionStage<Done> result =
        Source.fromIterator(() -> ids.iterator()).grouped(2).runWith(sink, materializer);
    // #delete-bulk-sink
  }

  private void UpsertSingleFlowSnippet() {
    ActorSystem system = ActorSystem.create();
    ActorMaterializer materializer = ActorMaterializer.create(system);
    Logger log = LoggerFactory.getLogger(Examples.class);

    CouchbaseCluster cluster = CouchbaseCluster.create("[Cluster Address]");
    cluster.authenticate(new PasswordAuthenticator("[Some Username]", "[Some password]"));
    Bucket queryBucket = cluster.openBucket("[some bucket name]");

    CouchbaseWriteSettings defaultCouchbaseWriteSettings = CouchbaseWriteSettings.create();

    // #upsertFlowSingle

    JsonDocument document =
        JsonDocument.create("id1", JsonObject.create().put("value", "some Value"));

    Flow<JsonDocument, SingleOperationResult<JsonDocument>, NotUsed> flow =
        CouchbaseFlow.upsertSingle(defaultCouchbaseWriteSettings, queryBucket);

    CompletionStage<Done> result =
        Source.single(document)
            .via(flow)
            .map(
                output -> {

                  // Do what you need with failure and return entity to the flow.
                  output.getException().ifPresent(ex -> log.error(output.entity().id(), ex));
                  return output.entity();
                })
            .runWith(Sink.ignore(), materializer);
    // #upsertFlowSingle
  }

  private void getByIdFlowSnippet() {
    ActorSystem system = ActorSystem.create();
    ActorMaterializer materializer = ActorMaterializer.create(system);

    Logger log = LoggerFactory.getLogger(Examples.class);

    CouchbaseCluster cluster = CouchbaseCluster.create("[Cluster Address]");
    cluster.authenticate(new PasswordAuthenticator("[Some Username]", "[Some password]"));
    Bucket couchbaseBucket = cluster.openBucket("[some bucket name]");

    // #by-single-id-flow
    String id = "First";

    Source<String, NotUsed> source = Source.single(id);
    Flow<String, JsonDocument, NotUsed> flow =
        CouchbaseFlow.fromSingleId(couchbaseBucket, JsonDocument.class);

    CompletionStage<JsonDocument> result = source.via(flow).runWith(Sink.head(), materializer);
    // #by-single-id-flow

  }

  private void getBulkByIdFlowSnippet() {
    ActorSystem system = ActorSystem.create();
    ActorMaterializer materializer = ActorMaterializer.create(system);

    Logger log = LoggerFactory.getLogger(Examples.class);

    CouchbaseCluster cluster = CouchbaseCluster.create("[Cluster Address]");
    cluster.authenticate(new PasswordAuthenticator("[Some Username]", "[Some password]"));
    Bucket couchbaseBucket = cluster.openBucket("[some bucket name]");

    // #by-bulk-id-flow
    List<String> ids = Arrays.asList("id1", "id2", "id3", "id4");

    Source<List<String>, NotUsed> source = Source.single(ids);
    Flow<List<String>, List<JsonDocument>, NotUsed> flow =
        CouchbaseFlow.fromBulkId(couchbaseBucket, JsonDocument.class);
    CompletionStage<List<JsonDocument>> result =
        source.via(flow).runWith(Sink.head(), materializer);
    // #by-bulk-id-flow

  }

  private void DeleteBulFlowSnippet() {
    ActorSystem system = ActorSystem.create();
    ActorMaterializer materializer = ActorMaterializer.create(system);

    Logger log = LoggerFactory.getLogger(Examples.class);

    CouchbaseCluster cluster = CouchbaseCluster.create("[Cluster Address]");
    cluster.authenticate(new PasswordAuthenticator("[Some Username]", "[Some password]"));
    Bucket queryBucket = cluster.openBucket("[some bucket name]");

    // #delete-bulk-flow
    CouchbaseWriteSettings defaultCouchbaseWriteSettings = CouchbaseWriteSettings.create();

    String id = "id1";
    String id2 = "id2";
    List<String> ids = Arrays.asList(id, id2);

    Flow<List<String>, BulkOperationResult<String>, NotUsed> flow =
        CouchbaseFlow.deleteBulk(defaultCouchbaseWriteSettings, queryBucket);
    CompletionStage<Done> result =
        Source.from(ids)
            .grouped(2)
            .via(flow)
            .map(
                output -> {
                  if (!output.failures().isEmpty()) {
                    // Do what you need with failure and return entity to the flow.
                    log.warn("There was problem to upsert some docs!!! ");
                  }
                  return output.entities();
                })
            .runWith(Sink.ignore(), materializer);
    // #delete-bulk-flow
  }
}
