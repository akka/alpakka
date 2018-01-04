/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.alpakka.elasticsearch.javadsl.*;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.elasticsearch.client.RestClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class ElasticsearchTest {

  private static ElasticsearchClusterRunner runner;
  private static RestClient client;
  private static ActorSystem system;
  private static ActorMaterializer materializer;

  //#define-class
  public static class Book {
    public String title;

    public Book() {}

    public Book(String title) {
      this.title = title;
    }
  }
  //#define-class

  @BeforeClass
  public static void setup() throws IOException {
    runner = new ElasticsearchClusterRunner();
    runner.build(ElasticsearchClusterRunner.newConfigs()
        .baseHttpPort(9200)
        .baseTransportPort(9300)
        .numOfNode(1)
        .disableESLogger());
    runner.ensureYellow();

    //#init-client
    client = RestClient.builder(new HttpHost("localhost", 9201)).build();
    //#init-client

    //#init-mat
    system = ActorSystem.create();
    materializer = ActorMaterializer.create(system);
    //#init-mat


    register("source", "Akka in Action");
    register("source", "Programming in Scala");
    register("source", "Learning Scala");
    register("source", "Scala for Spark in Production");
    register("source", "Scala Puzzlers");
    register("source", "Effective Akka");
    register("source", "Akka Concurrency");
    flush("source");
  }

  @AfterClass
  public static void teardown() throws Exception {
    runner.close();
    runner.clean();
    client.close();
    TestKit.shutdownActorSystem(system);
  }


  private static void flush(String indexName) throws IOException {
    client.performRequest("POST", indexName + "/_flush");
  }

  private static void register(String indexName, String title) throws IOException {
    client.performRequest("POST",
    indexName + "/book",
    new HashMap<>(),
    new StringEntity(String.format("{\"title\": \"%s\"}", title)),
    new BasicHeader("Content-Type", "application/json"));
  }


  @Test
  public void jsObjectStream() throws Exception {
    // Copy source/book to sink1/book through JsObject stream
    //#run-jsobject
    CompletionStage<Done> f1 = ElasticsearchSource.create(
      "source",
      "book",
      "{\"match_all\": {}}",
      new ElasticsearchSourceSettings().withBufferSize(5),
      client)
      .map(m -> IncomingMessage.create(m.id(), m.source()))
      .runWith(
        ElasticsearchSink.create(
          "sink1",
          "book",
          new ElasticsearchSinkSettings().withBufferSize(5),
          client,
          new ObjectMapper()),
        materializer);
    //#run-jsobject

    f1.toCompletableFuture().get();

    flush("sink1");

    // Assert docs in sink1/book
    CompletionStage<List<String>> f2 = ElasticsearchSource.create(
      "sink1",
      "book",
      "{\"match_all\": {}}",
      new ElasticsearchSourceSettings(5),
      client)
    .map(m -> (String) m.source().get("title"))
    .runWith(Sink.seq(), materializer);

    List<String> result = new ArrayList<>(f2.toCompletableFuture().get());

    List<String> expect = Arrays.asList(
      "Akka Concurrency",
      "Akka in Action",
      "Effective Akka",
      "Learning Scala",
      "Programming in Scala",
      "Scala Puzzlers",
      "Scala for Spark in Production"
    );

    Collections.sort(result);
    assertEquals(expect, result);
  }

  @Test
  public void typedStream() throws Exception {
    // Copy source/book to sink2/book through JsObject stream
    //#run-typed
    CompletionStage<Done> f1 = ElasticsearchSource.typed(
      "source",
      "book",
      "{\"match_all\": {}}",
      new ElasticsearchSourceSettings().withBufferSize(5),
      client,
      Book.class)
      .map(m -> IncomingMessage.create(m.id(), m.source()))
      .runWith(
        ElasticsearchSink.create(
          "sink2",
          "book",
          new ElasticsearchSinkSettings().withBufferSize(5),
          client,
          new ObjectMapper()),
        materializer);
    //#run-typed

    f1.toCompletableFuture().get();

    flush("sink2");

    // Assert docs in sink2/book
    CompletionStage<List<String>> f2 = ElasticsearchSource.typed(
      "sink2",
      "book",
      "{\"match_all\": {}}",
      new ElasticsearchSourceSettings().withBufferSize(5),
      client,
      Book.class)
      .map(m -> m.source().title)
      .runWith(Sink.seq(), materializer);

    List<String> result = new ArrayList<>(f2.toCompletableFuture().get());

    List<String> expect = Arrays.asList(
      "Akka Concurrency",
      "Akka in Action",
      "Effective Akka",
      "Learning Scala",
      "Programming in Scala",
      "Scala Puzzlers",
      "Scala for Spark in Production"
    );

    Collections.sort(result);
    assertEquals(expect, result);
  }

  @Test
  public void flow() throws Exception {
    // Copy source/book to sink3/book through JsObject stream
    //#run-flow
    CompletionStage<List<List<IncomingMessageResult<Book, NotUsed>>>> f1 = ElasticsearchSource.typed(
        "source",
        "book",
        "{\"match_all\": {}}",
        new ElasticsearchSourceSettings().withBufferSize(5),
        client,
        Book.class)
        .map(m -> IncomingMessage.create(m.id(), m.source()))
        .via(ElasticsearchFlow.create(
                "sink3",
                "book",
                new ElasticsearchSinkSettings().withBufferSize(5),
                client,
                new ObjectMapper()))
        .runWith(Sink.seq(), materializer);
    //#run-flow

    List<List<IncomingMessageResult<Book, NotUsed>>> result1 = f1.toCompletableFuture().get();
    flush("sink3");

    for (List<IncomingMessageResult<Book, NotUsed>> aResult1 : result1) {
      assertEquals(true, aResult1.get(0).success());
    }

    // Assert docs in sink3/book
    CompletionStage<List<String>> f2 = ElasticsearchSource.typed(
        "sink3",
        "book",
        "{\"match_all\": {}}",
        new ElasticsearchSourceSettings().withBufferSize(5),
        client,
        Book.class)
        .map(m -> m.source().title)
        .runWith(Sink.seq(), materializer);

    List<String> result2 = new ArrayList<>(f2.toCompletableFuture().get());

    List<String> expect = Arrays.asList(
        "Akka Concurrency",
        "Akka in Action",
        "Effective Akka",
        "Learning Scala",
        "Programming in Scala",
        "Scala Puzzlers",
        "Scala for Spark in Production"
    );

    Collections.sort(result2);
    assertEquals(expect, result2);
  }

  @Test
  public void testKafkaExample() throws Exception {
    //#kafka-example
    // We're going to pretend we got messages from kafka.
    // After we've written them to Elastic, we want
    // to commit the offset to Kafka

    List<KafkaMessage> messagesFromKafka = Arrays.asList(
            new KafkaMessage(new Book("Book 1"), new KafkaOffset(0)),
            new KafkaMessage(new Book("Book 2"), new KafkaOffset(1)),
            new KafkaMessage(new Book("Book 3"), new KafkaOffset(2))
    );

    final KafkaCommitter kafkaCommitter = new KafkaCommitter();

    Source.from(messagesFromKafka) // Assume we get this from Kafka
            .map(kafkaMessage -> {
              Book book = kafkaMessage.book;
              String id = book.title;

              // Transform message so that we can write to elastic
              return IncomingMessage.create(id, book, kafkaMessage.offset);
            })
            .via( // write to elastic
                    ElasticsearchFlow.createWithPassThrough(
                            "sink6",
                            "book",
                            new ElasticsearchSinkSettings().withBufferSize(5),
                            client,
                            new ObjectMapper())
            ).map(messageResults -> {
              messageResults.stream()
                      .forEach(result -> {
                        if (!result.success()) throw new RuntimeException("Failed to write message to elastic");
                        // Commit to kafka
                        kafkaCommitter.commit(result.passThrough());
                      });
              return NotUsed.getInstance();

            }).runWith(Sink.seq(), materializer) // Run it
            .toCompletableFuture().get(); // Wait for it to complete

    //#kafka-example
    flush("sink6");

    // Make sure all messages was committed to kafka
    assertEquals (Arrays.asList(0,1,2), kafkaCommitter.committedOffsets);


    // Assert that all docs were written to elastic
    List<String> result2 = ElasticsearchSource.typed(
            "sink6",
            "book",
            "{\"match_all\": {}}",
             new ElasticsearchSourceSettings(),
             client,
             Book.class)
            .map( m -> m.source().title)
            .runWith(Sink.seq(), materializer) // Run it
            .toCompletableFuture().get(); // Wait for it to complete

    assertEquals(
            messagesFromKafka.stream().map(m -> m.book.title).sorted().collect(Collectors.toList()),
            result2.stream().sorted().collect(Collectors.toList())
    );

  }

  static class KafkaCommitter {
    List<Integer> committedOffsets = new ArrayList<>();

    public KafkaCommitter() {
    }

    void commit(KafkaOffset offset) {
      committedOffsets.add(offset.offset);
    }
  }

  static class KafkaOffset {
    final int offset;

    public KafkaOffset(int offset) {
      this.offset = offset;
    }

  }

  static class KafkaMessage {
    final Book book;
    final KafkaOffset offset;

    public KafkaMessage(Book book, KafkaOffset offset) {
      this.book = book;
      this.offset = offset;
    }

  }
}