/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.elasticsearch;

import akka.Done;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.alpakka.elasticsearch.javadsl.ElasticsearchSink;
import akka.stream.alpakka.elasticsearch.javadsl.ElasticsearchSource;
import akka.stream.javadsl.Sink;
import akka.testkit.JavaTestKit;
import org.apache.http.HttpHost;
import org.apache.http.entity.StringEntity;
import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.elasticsearch.client.RestClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Some;
import spray.json.JsString;
import spray.json.JsValue;
import spray.json.JsonFormat;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletionStage;

import static org.junit.Assert.assertEquals;
//#import-spray-json-support
import static akka.stream.alpakka.elasticsearch.javadsl.SprayJsonSupport.*;
//#import-spray-json-support

public class ElasticsearchTest {

  private static ElasticsearchClusterRunner runner;
  private static RestClient client;
  private static ActorSystem system;
  private static ActorMaterializer materializer;

  //#define-jsonformat
  class Book {
    private String title;

    public Book(String title){
      this.title = title;
    }

    public String getTitie(){
      return this.title;
    }
  }

  JsonFormat<Book> format = new JsonFormat<Book>() {
    @Override
    public Book read(JsValue json) {
      return new Book(new JsonObject(json.asJsObject()).getField("title").getStringValue());
    }

    @Override
    public JsValue write(Book obj) {
      return new JsonObject()
        .addField(new JsonField("title", new JsonString(obj.getTitie())))
        .toJson();
    }
  };
  //#define-jsonformat

  @BeforeClass
  public static void setup() throws IOException {
    runner = new ElasticsearchClusterRunner();
    runner.build(ElasticsearchClusterRunner.newConfigs().baseHttpPort(9210).baseTransportPort(9310).numOfNode(1));
    runner.ensureYellow();

    //#init-client
    client = RestClient.builder(new HttpHost("localhost", 9211)).build();
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
    new StringEntity(String.format("{\"title\": \"%s\"}", title)));
  }


  @Test
  public void jsObjectStream() throws Exception {
    // Copy source/book to sink1/book through JsObject stream
    //#run-jsobject
    CompletionStage<Done> f1 = ElasticsearchSource.create(
      "source",
      "book",
      "{\"match_all\": {}}",
      new ElasticsearchSourceSettings(5),
      client)
      .map(m -> new IncomingMessage<>(new Some<String>(m.id()), m.source()))
      .runWith(
        ElasticsearchSink.create(
          "sink1",
          "book",
          new ElasticsearchSinkSettings(5),
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
    .map(m -> ((JsString) m.source().fields().apply("title")).value())
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
    // Copy source/book to sink1/book through JsObject stream
    //#run-typed
    CompletionStage<Done> f1 = ElasticsearchSource.typed(
      "source",
      "book",
      "{\"match_all\": {}}",
      new ElasticsearchSourceSettings(5),
      client,
      format)
      .map(m -> new IncomingMessage<>(new Some<String>(m.id()), m.source()))
      .runWith(
        ElasticsearchSink.typed(
          "sink1",
          "book",
          new ElasticsearchSinkSettings(5),
          client,
          format),
        materializer);
    //#run-typed

    f1.toCompletableFuture().get();

    flush("sink1");

    // Assert docs in sink1/book
    CompletionStage<List<String>> f2 = ElasticsearchSource.typed(
      "sink1",
      "book",
      "{\"match_all\": {}}",
      new ElasticsearchSourceSettings(5),
      client,
      format)
      .map(m -> m.source().getTitie())
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

}