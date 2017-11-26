/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch;

import akka.Done;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.alpakka.elasticsearch.javadsl.*;
import akka.stream.javadsl.Sink;
import akka.testkit.JavaTestKit;
import org.apache.http.HttpHost;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.elasticsearch.client.RestClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Some;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletionStage;

import static org.junit.Assert.assertEquals;

public class ElasticsearchTest {

  private static ElasticsearchClusterRunner runner;
  private static RestClient client;
  private static ActorSystem system;
  private static ActorMaterializer materializer;

  //#define-class
  public static class Book {
    public String title;
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
    JavaTestKit.shutdownActorSystem(system);
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
      .map(m -> new IncomingMessage<>(new Some<String>(m.id()), m.source()))
      .runWith(
        ElasticsearchSink.create(
          "sink1",
          "book",
          new ElasticsearchSinkSettings().withBufferSize(5),
          client),
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
      .map(m -> new IncomingMessage<>(new Some<String>(m.id()), m.source()))
      .runWith(
        ElasticsearchSink.typed(
          "sink2",
          "book",
          new ElasticsearchSinkSettings().withBufferSize(5),
          client),
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
    CompletionStage<List<List<IncomingMessage<Book>>>> f1 = ElasticsearchSource.typed(
        "source",
        "book",
        "{\"match_all\": {}}",
        new ElasticsearchSourceSettings().withBufferSize(5),
        client,
        Book.class)
        .map(m -> new IncomingMessage<>(new Some<String>(m.id()), m.source()))
        .via(ElasticsearchFlow.typed(
                "sink3",
                "book",
                new ElasticsearchSinkSettings().withBufferSize(5),
                client))
        .runWith(Sink.seq(), materializer);
    //#run-flow

    List<List<IncomingMessage<Book>>> result1 = f1.toCompletableFuture().get();
    flush("sink3");

    for(int i = 0; i < result1.size(); i ++){
      assertEquals(true, result1.get(i).isEmpty());
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

}