/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.actor.ActorSystem;
import akka.http.javadsl.Http;
import akka.http.javadsl.model.ContentTypes;
import akka.http.javadsl.model.HttpRequest;
import akka.stream.ActorMaterializer;
import akka.stream.alpakka.elasticsearch.ApiVersion;
import akka.stream.alpakka.elasticsearch.ElasticsearchConnectionSettings;
import akka.stream.alpakka.elasticsearch.ElasticsearchParams;
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import akka.stream.testkit.javadsl.StreamTestKit;
import akka.testkit.javadsl.TestKit;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ElasticsearchTestBase {
  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  protected static ApiVersion apiVersion;
  protected static ElasticsearchConnectionSettings connectionSettings;
  protected static ActorSystem system;
  protected static ActorMaterializer materializer;
  protected static Http http;

  // #define-class
  public static class Book {
    public String title;

    public Book() {}

    public Book(String title) {
      this.title = title;
    }
  }
  // #define-class

  @BeforeClass
  public static void setupBase() throws IOException {
    // #init-mat
    system = ActorSystem.create();
    materializer = ActorMaterializer.create(system);
    // #init-mat
    http = Http.get(system);
  }

  @AfterClass
  public static void teardown() throws Exception {
    TestKit.shutdownActorSystem(system);
  }

  @After
  public void checkForStageLeaks() {
    StreamTestKit.assertAllStagesStopped(materializer);
  }

  protected static void prepareIndex(int port, ApiVersion esApiVersion) throws IOException {
    apiVersion = esApiVersion;

    connectionSettings =
        ElasticsearchConnectionSettings.create(String.format("http://localhost:%d", port));

    register("source", "Akka in Action");
    register("source", "Programming in Scala");
    register("source", "Learning Scala");
    register("source", "Scala for Spark in Production");
    register("source", "Scala Puzzlers");
    register("source", "Effective Akka");
    register("source", "Akka Concurrency");
    flushAndRefresh("source");
  }

  protected static void cleanIndex() throws IOException {
    HttpRequest request =
        HttpRequest.DELETE(String.format("%s/_all", connectionSettings.baseUrl()));
    http.singleRequest(request).toCompletableFuture().join();
  }

  protected static void flushAndRefresh(String indexName) throws IOException {
    HttpRequest flushRequest =
        HttpRequest.POST(String.format("%s/%s/_flush", connectionSettings.baseUrl(), indexName));
    http.singleRequest(flushRequest).toCompletableFuture().join();

    HttpRequest refreshRequest =
        HttpRequest.POST(String.format("%s/%s/_refresh", connectionSettings.baseUrl(), indexName));
    http.singleRequest(refreshRequest).toCompletableFuture().join();
  }

  protected static void register(String indexName, String title) {
    HttpRequest request =
        HttpRequest.POST(String.format("%s/%s/_doc", connectionSettings.baseUrl(), indexName))
            .withEntity(ContentTypes.APPLICATION_JSON, String.format("{\"title\": \"%s\"}", title));

    http.singleRequest(request).toCompletableFuture().join();
  }

  // #custom-search-params
  public static class TestDoc {
    public String id;
    public String a;
    public String b;
    public String c;

    // #custom-search-params
    public TestDoc() {}

    public TestDoc(String id, String a, String b, String c) {
      this.id = id;
      this.a = a;
      this.b = b;
      this.c = c;
    }
    // #custom-search-params
  }
  // #custom-search-params

  static class KafkaCommitter {
    List<Integer> committedOffsets = new ArrayList<>();

    public KafkaCommitter() {}

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

  protected ElasticsearchParams constructElasticsearchParams(
      String indexName, String typeName, ApiVersion apiVersion) {
    if (apiVersion == ApiVersion.V5) {
      return ElasticsearchParams.V5(indexName, typeName);
    } else {
      return ElasticsearchParams.V7(indexName);
    }
  }
}
