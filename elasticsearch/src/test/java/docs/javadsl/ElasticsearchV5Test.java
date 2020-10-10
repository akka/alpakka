/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.stream.alpakka.elasticsearch.*;
import akka.stream.alpakka.elasticsearch.javadsl.ElasticsearchFlow;
import akka.stream.alpakka.elasticsearch.javadsl.ElasticsearchSink;
import akka.stream.alpakka.elasticsearch.javadsl.ElasticsearchSource;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class ElasticsearchV5Test extends ElasticsearchTestBase {
  @BeforeClass
  public static void setup() throws IOException {
    setupBase();

    prepareIndex(9201, ApiVersion.V5);
  }

  @AfterClass
  public static void shutdown() throws IOException {
    cleanIndex();
  }

  @Test
  public void typedStream() throws Exception {
    // Copy source/book to sink2/book through JsObject stream
    // #run-typed
    ElasticsearchSourceSettings sourceSettings =
        ElasticsearchSourceSettings.create(connectionSettings).withApiVersion(ApiVersion.V5);
    ElasticsearchWriteSettings sinkSettings =
        ElasticsearchWriteSettings.create(connectionSettings).withApiVersion(ApiVersion.V5);

    Source<ReadResult<ElasticsearchTestBase.Book>, NotUsed> source =
        ElasticsearchSource.typed(
            constructEsParams("source", "_doc", ApiVersion.V5),
            "{\"match_all\": {}}",
            sourceSettings,
            ElasticsearchTestBase.Book.class);
    CompletionStage<Done> f1 =
        source
            .map(m -> WriteMessage.createIndexMessage(m.id(), m.source()))
            .runWith(
                ElasticsearchSink.create(
                    constructEsParams("sink2", "_doc", ApiVersion.V5),
                    sinkSettings,
                    new ObjectMapper()),
                materializer);
    // #run-typed

    f1.toCompletableFuture().get();

    flushAndRefresh("sink2");

    // Assert docs in sink2/book
    CompletionStage<List<String>> f2 =
        ElasticsearchSource.typed(
                constructEsParams("sink2", "_doc", ApiVersion.V5),
                "{\"match_all\": {}}",
                ElasticsearchSourceSettings.create(connectionSettings)
                    .withApiVersion(ApiVersion.V5)
                    .withBufferSize(5),
                ElasticsearchTestBase.Book.class)
            .map(m -> m.source().title)
            .runWith(Sink.seq(), materializer);

    List<String> result = new ArrayList<>(f2.toCompletableFuture().get());

    List<String> expect =
        Arrays.asList(
            "Akka Concurrency",
            "Akka in Action",
            "Effective Akka",
            "Learning Scala",
            "Programming in Scala",
            "Scala Puzzlers",
            "Scala for Spark in Production");

    Collections.sort(result);
    assertEquals(expect, result);
  }

  @Test
  public void jsObjectStream() throws Exception {
    // Copy source/book to sink1/book through JsObject stream
    // #run-jsobject
    ElasticsearchSourceSettings sourceSettings =
        ElasticsearchSourceSettings.create(connectionSettings).withApiVersion(ApiVersion.V5);
    ElasticsearchWriteSettings sinkSettings =
        ElasticsearchWriteSettings.create(connectionSettings).withApiVersion(ApiVersion.V5);

    Source<ReadResult<Map<String, Object>>, NotUsed> source =
        ElasticsearchSource.create(
            constructEsParams("source", "_doc", ApiVersion.V5),
            "{\"match_all\": {}}",
            sourceSettings);
    CompletionStage<Done> f1 =
        source
            .map(m -> WriteMessage.createIndexMessage(m.id(), m.source()))
            .runWith(
                ElasticsearchSink.create(
                    constructEsParams("sink1", "_doc", ApiVersion.V5),
                    sinkSettings,
                    new ObjectMapper()),
                materializer);
    // #run-jsobject

    f1.toCompletableFuture().get();

    flushAndRefresh("sink1");

    // Assert docs in sink1/_doc
    CompletionStage<List<String>> f2 =
        ElasticsearchSource.create(
                constructEsParams("sink1", "_doc", ApiVersion.V5),
                "{\"match_all\": {}}",
                ElasticsearchSourceSettings.create(connectionSettings)
                    .withApiVersion(ApiVersion.V5)
                    .withBufferSize(5))
            .map(m -> (String) m.source().get("title"))
            .runWith(Sink.seq(), materializer);

    List<String> result = new ArrayList<>(f2.toCompletableFuture().get());

    List<String> expect =
        Arrays.asList(
            "Akka Concurrency",
            "Akka in Action",
            "Effective Akka",
            "Learning Scala",
            "Programming in Scala",
            "Scala Puzzlers",
            "Scala for Spark in Production");

    Collections.sort(result);
    assertEquals(expect, result);
  }

  @Test
  public void flow() throws Exception {
    // Copy source/book to sink3/book through JsObject stream
    // #run-flow
    CompletionStage<List<WriteResult<Book, NotUsed>>> f1 =
        ElasticsearchSource.typed(
                constructEsParams("source", "_doc", ApiVersion.V5),
                "{\"match_all\": {}}",
                ElasticsearchSourceSettings.create(connectionSettings)
                    .withApiVersion(ApiVersion.V5)
                    .withBufferSize(5),
                Book.class)
            .map(m -> WriteMessage.createIndexMessage(m.id(), m.source()))
            .via(
                ElasticsearchFlow.create(
                    constructEsParams("sink3", "_doc", ApiVersion.V5),
                    ElasticsearchWriteSettings.create(connectionSettings)
                        .withApiVersion(ApiVersion.V5)
                        .withBufferSize(5),
                    new ObjectMapper()))
            .runWith(Sink.seq(), materializer);
    // #run-flow

    List<WriteResult<Book, NotUsed>> result1 = f1.toCompletableFuture().get();
    flushAndRefresh("sink3");

    for (WriteResult<Book, NotUsed> aResult1 : result1) {
      assertEquals(true, aResult1.success());
    }

    // Assert docs in sink3/book
    CompletionStage<List<String>> f2 =
        ElasticsearchSource.typed(
                constructEsParams("sink3", "_doc", ApiVersion.V5),
                "{\"match_all\": {}}",
                ElasticsearchSourceSettings.create(connectionSettings)
                    .withApiVersion(ApiVersion.V5)
                    .withConnection(connectionSettings)
                    .withBufferSize(5),
                Book.class)
            .map(m -> m.source().title)
            .runWith(Sink.seq(), materializer);

    List<String> result2 = new ArrayList<>(f2.toCompletableFuture().get());

    List<String> expect =
        Arrays.asList(
            "Akka Concurrency",
            "Akka in Action",
            "Effective Akka",
            "Learning Scala",
            "Programming in Scala",
            "Scala Puzzlers",
            "Scala for Spark in Production");

    Collections.sort(result2);
    assertEquals(expect, result2);
  }

  @Test
  public void stringFlow() throws Exception {
    // Copy source/book to sink3/book through JsObject stream
    // #string
    String indexName = "sink3-0";
    CompletionStage<List<WriteResult<String, NotUsed>>> write =
        Source.from(
                Arrays.asList(
                    WriteMessage.createIndexMessage("1", "{\"title\": \"Das Parfum\"}"),
                    WriteMessage.createIndexMessage("2", "{\"title\": \"Faust\"}"),
                    WriteMessage.createIndexMessage(
                        "3", "{\"title\": \"Die unendliche Geschichte\"}")))
            .via(
                ElasticsearchFlow.create(
                    constructEsParams(indexName, "_doc", ApiVersion.V5),
                    ElasticsearchWriteSettings.create(connectionSettings)
                        .withApiVersion(ApiVersion.V5)
                        .withBufferSize(5),
                    StringMessageWriter.getInstance()))
            .runWith(Sink.seq(), materializer);
    // #string

    List<WriteResult<String, NotUsed>> result1 = write.toCompletableFuture().get();
    flushAndRefresh(indexName);

    for (WriteResult<String, NotUsed> aResult1 : result1) {
      assertEquals(true, aResult1.success());
    }

    CompletionStage<List<String>> f2 =
        ElasticsearchSource.typed(
                constructEsParams(indexName, "_doc", ApiVersion.V5),
                "{\"match_all\": {}}",
                ElasticsearchSourceSettings.create(connectionSettings)
                    .withApiVersion(ApiVersion.V5)
                    .withBufferSize(5),
                Book.class)
            .map(m -> m.source().title)
            .runWith(Sink.seq(), materializer);

    List<String> result2 = new ArrayList<>(f2.toCompletableFuture().get());

    List<String> expect = Arrays.asList("Das Parfum", "Die unendliche Geschichte", "Faust");

    Collections.sort(result2);
    assertEquals(expect, result2);
  }

  @Test
  public void testMultipleOperations() throws Exception {
    // #multiple-operations
    // Create, update, upsert and delete documents in sink8/book
    List<WriteMessage<Book, NotUsed>> requests =
        Arrays.asList(
            WriteMessage.createIndexMessage("00001", new Book("Book 1")),
            WriteMessage.createUpsertMessage("00002", new Book("Book 2")),
            WriteMessage.createUpsertMessage("00003", new Book("Book 3")),
            WriteMessage.createUpdateMessage("00004", new Book("Book 4")),
            WriteMessage.createDeleteMessage("00002"));

    Source.from(requests)
        .via(
            ElasticsearchFlow.create(
                constructEsParams("sink8", "_doc", ApiVersion.V5),
                ElasticsearchWriteSettings.create(connectionSettings).withApiVersion(ApiVersion.V5),
                new ObjectMapper()))
        .runWith(Sink.seq(), materializer)
        .toCompletableFuture()
        .get();
    // #multiple-operations

    flushAndRefresh("sink8");

    // Assert docs in sink8/book
    CompletionStage<List<String>> f2 =
        ElasticsearchSource.typed(
                constructEsParams("sink8", "_doc", ApiVersion.V5),
                "{\"match_all\": {}}",
                ElasticsearchSourceSettings.create(connectionSettings)
                    .withApiVersion(ApiVersion.V5),
                Book.class)
            .map(m -> m.source().title)
            .runWith(Sink.seq(), materializer);

    List<String> result2 = new ArrayList<>(f2.toCompletableFuture().get());
    List<String> expect = Arrays.asList("Book 1", "Book 3");
    Collections.sort(result2);

    assertEquals(expect, result2);
  }

  @Test
  public void testKafkaExample() throws Exception {
    // #kafka-example
    // We're going to pretend we got messages from kafka.
    // After we've written them to Elastic, we want
    // to commit the offset to Kafka

    List<KafkaMessage> messagesFromKafka =
        Arrays.asList(
            new KafkaMessage(new Book("Book 1"), new KafkaOffset(0)),
            new KafkaMessage(new Book("Book 2"), new KafkaOffset(1)),
            new KafkaMessage(new Book("Book 3"), new KafkaOffset(2)));

    final KafkaCommitter kafkaCommitter = new KafkaCommitter();

    CompletionStage<Done> kafkaToEs =
        Source.from(messagesFromKafka) // Assume we get this from Kafka
            .map(
                kafkaMessage -> {
                  Book book = kafkaMessage.book;
                  String id = book.title;

                  // Transform message so that we can write to elastic
                  return WriteMessage.createIndexMessage(id, book)
                      .withPassThrough(kafkaMessage.offset);
                })
            .via( // write to elastic
                ElasticsearchFlow.createWithPassThrough(
                    constructEsParams("sink6", "_doc", ApiVersion.V5),
                    ElasticsearchWriteSettings.create(connectionSettings)
                        .withApiVersion(ApiVersion.V5)
                        .withBufferSize(5),
                    new ObjectMapper()))
            .map(
                result -> {
                  if (!result.success())
                    throw new RuntimeException("Failed to write message to elastic");
                  // Commit to kafka
                  kafkaCommitter.commit(result.message().passThrough());
                  return NotUsed.getInstance();
                })
            .runWith(Sink.ignore(), materializer);
    // #kafka-example
    kafkaToEs.toCompletableFuture().get(5, TimeUnit.SECONDS); // Wait for it to complete
    flushAndRefresh("sink6");

    // Make sure all messages was committed to kafka
    assertEquals(Arrays.asList(0, 1, 2), kafkaCommitter.committedOffsets);

    // Assert that all docs were written to elastic
    List<String> result2 =
        ElasticsearchSource.typed(
                constructEsParams("sink6", "_doc", ApiVersion.V5),
                "{\"match_all\": {}}",
                ElasticsearchSourceSettings.create(connectionSettings)
                    .withApiVersion(ApiVersion.V5),
                Book.class)
            .map(m -> m.source().title)
            .runWith(Sink.seq(), materializer) // Run it
            .toCompletableFuture()
            .get(); // Wait for it to complete

    assertEquals(
        messagesFromKafka.stream().map(m -> m.book.title).sorted().collect(Collectors.toList()),
        result2.stream().sorted().collect(Collectors.toList()));
  }

  @Test
  public void testUsingSearchParams() throws Exception {

    String indexName = "test_using_search_params_versions_java";
    String typeName = "_doc";

    List<TestDoc> docs =
        Arrays.asList(
            new TestDoc("1", "a1", "b1", "c1"),
            new TestDoc("2", "a2", "b2", "c2"),
            new TestDoc("3", "a3", "b3", "c3"));

    // Insert document
    Source.from(docs)
        .map((TestDoc d) -> WriteMessage.createIndexMessage(d.id, d))
        .via(
            ElasticsearchFlow.create(
                constructEsParams(indexName, typeName, ApiVersion.V5),
                ElasticsearchWriteSettings.create(connectionSettings)
                    .withApiVersion(ApiVersion.V5)
                    .withBufferSize(5),
                new ObjectMapper()))
        .runWith(Sink.seq(), materializer)
        .toCompletableFuture()
        .get();

    flushAndRefresh(indexName);

    // #custom-search-params
    // Search for docs and ask elastic to only return some fields

    Map<String, String> searchParams = new HashMap<>();
    searchParams.put("query", "{\"match_all\": {}}");
    searchParams.put("_source", "[\"id\", \"a\", \"c\"]");

    List<TestDoc> result =
        ElasticsearchSource.<TestDoc>typed(
                constructEsParams(indexName, typeName, ApiVersion.V5),
                searchParams, // <-- Using searchParams
                ElasticsearchSourceSettings.create(connectionSettings)
                    .withApiVersion(ApiVersion.V5),
                TestDoc.class,
                new ObjectMapper())
            .map(
                o -> {
                  return o.source(); // These documents will only have property id, a and c (not
                })
            .runWith(Sink.seq(), materializer)
            .toCompletableFuture()
            .get();
    // #custom-search-params
    flushAndRefresh(indexName);

    assertEquals(
        docs.size(),
        result.stream()
            .filter(
                d -> {
                  return d.a != null && d.b == null;
                })
            .collect(Collectors.toList())
            .size());
  }
}
