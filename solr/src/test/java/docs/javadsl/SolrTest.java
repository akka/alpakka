/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
// #solr-update-settings
import akka.stream.alpakka.solr.SolrUpdateSettings;
// #solr-update-settings
import akka.stream.alpakka.solr.WriteMessage;
import akka.stream.alpakka.solr.javadsl.SolrFlow;
import akka.stream.alpakka.solr.javadsl.SolrSink;
import akka.stream.alpakka.solr.javadsl.SolrSource;
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.beans.Field;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.ZkClientClusterStateProvider;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.CloudSolrStream;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParser;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.ZkTestServer;
import org.apache.solr.common.SolrInputDocument;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("deprecation") // FIXME #2917 Deprecated getIdField in Solrj 8.11.x
public class SolrTest {
  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  private static MiniSolrCloudCluster cluster;
  private static SolrClient solrClient;
  private static SolrClient cl;
  private static ActorSystem system = ActorSystem.create();
  // #init-client
  private static final int zookeeperPort = 9984;
  private static final String zookeeperHost = "127.0.0.1:" + zookeeperPort + "/solr";
  // #init-client
  private static ZkTestServer zkTestServer;
  private static String predefinedCollection = "collection1";

  // #define-class
  public static class Book {
    public String title;

    public String comment;

    public String router;

    public Book() {}

    public Book(String title) {
      this.title = title;
    }

    public Book(String title, String comment) {
      this.title = title;
      this.comment = comment;
    }

    public Book(String title, String comment, String router) {
      this.title = title;
      this.comment = comment;
      this.router = router;
    }
  }

  Function<Book, SolrInputDocument> bookToDoc =
      book -> {
        SolrInputDocument doc = new SolrInputDocument();
        doc.setField("title", book.title);
        doc.setField("comment", book.comment);
        if (book.router != null) doc.setField("router", book.router);
        return doc;
      };

  Function<Tuple, Book> tupleToBook =
      tuple -> {
        String title = tuple.getString("title");
        return new Book(title, tuple.getString("comment"));
      };
  // #define-class

  @Test
  public void solrInputDocumentStream() throws Exception {
    String collectionName = createCollection();
    TupleStream stream = getTupleStream(predefinedCollection);

    // #run-document
    CompletionStage<UpdateResponse> copyCollection =
        SolrSource.fromTupleStream(stream)
            .map(
                tuple -> {
                  Book book = tupleToBook.apply(tuple);
                  SolrInputDocument doc = bookToDoc.apply(book);
                  return WriteMessage.createUpsertMessage(doc);
                })
            .groupedWithin(5, Duration.ofMillis(10))
            .runWith(
                SolrSink.documents(collectionName, SolrUpdateSettings.create(), solrClient), system)
            // explicit commit when stream ended
            .thenApply(
                done -> {
                  try {
                    return solrClient.commit(collectionName);
                  } catch (Exception e) {
                    throw new IllegalStateException(e);
                  }
                });
    // #run-document

    resultOf(copyCollection);

    TupleStream stream2 = getTupleStream(collectionName);

    CompletionStage<List<String>> res2 =
        SolrSource.fromTupleStream(stream2)
            .map(t -> tupleToBook.apply(t).title)
            .runWith(Sink.seq(), system);

    List<String> result = new ArrayList<>(resultOf(res2));

    List<String> expect =
        Arrays.asList(
            "Akka Concurrency",
            "Akka in Action",
            "Effective Akka",
            "Learning Scala",
            "Programming in Scala",
            "Scala Puzzlers",
            "Scala for Spark in Production");

    assertEquals(expect, result);
  }

  @Test
  public void beanStream() throws Exception {
    String collectionName = createCollection();
    TupleStream stream = getTupleStream(predefinedCollection);

    // #define-bean
    class BookBean {
      @Field("title")
      public String title;

      public BookBean(String title) {
        this.title = title;
      }
    }
    // #define-bean

    // #run-bean
    CompletionStage<UpdateResponse> copyCollection =
        SolrSource.fromTupleStream(stream)
            .map(
                tuple -> {
                  String title = tuple.getString("title");
                  return WriteMessage.createUpsertMessage(new BookBean(title));
                })
            .groupedWithin(5, Duration.ofMillis(10))
            .runWith(
                SolrSink.beans(
                    collectionName, SolrUpdateSettings.create(), solrClient, BookBean.class),
                system)
            // explicit commit when stream ended
            .thenApply(
                done -> {
                  try {
                    return solrClient.commit(collectionName);
                  } catch (Exception e) {
                    throw new IllegalStateException(e);
                  }
                });
    // #run-bean

    resultOf(copyCollection);

    TupleStream stream2 = getTupleStream(collectionName);

    CompletionStage<List<String>> res2 =
        SolrSource.fromTupleStream(stream2)
            .map(t -> tupleToBook.apply(t).title)
            .runWith(Sink.seq(), system);

    List<String> result = new ArrayList<>(resultOf(res2));

    List<String> expect =
        Arrays.asList(
            "Akka Concurrency",
            "Akka in Action",
            "Effective Akka",
            "Learning Scala",
            "Programming in Scala",
            "Scala Puzzlers",
            "Scala for Spark in Production");

    assertEquals(expect, result);
  }

  @Test
  public void typedStream() throws Exception {
    String collectionName = createCollection();
    TupleStream stream = getTupleStream(predefinedCollection);

    // #run-typed
    CompletionStage<UpdateResponse> copyCollection =
        SolrSource.fromTupleStream(stream)
            .map(tuple -> WriteMessage.createUpsertMessage(tupleToBook.apply(tuple)))
            .groupedWithin(5, Duration.ofMillis(10))
            .runWith(
                SolrSink.typeds(
                    collectionName, SolrUpdateSettings.create(), bookToDoc, solrClient, Book.class),
                system)
            // explicit commit when stream ended
            .thenApply(
                done -> {
                  try {
                    return solrClient.commit(collectionName);
                  } catch (Exception e) {
                    throw new IllegalStateException(e);
                  }
                });
    // #run-typed

    resultOf(copyCollection);

    TupleStream stream2 = getTupleStream(collectionName);

    CompletionStage<List<String>> res2 =
        SolrSource.fromTupleStream(stream2)
            .map(t -> tupleToBook.apply(t).title)
            .runWith(Sink.seq(), system);

    List<String> result = new ArrayList<>(resultOf(res2));

    List<String> expect =
        Arrays.asList(
            "Akka Concurrency",
            "Akka in Action",
            "Effective Akka",
            "Learning Scala",
            "Programming in Scala",
            "Scala Puzzlers",
            "Scala for Spark in Production");

    assertEquals(expect, result);
  }

  @Test
  public void flow() throws Exception {
    String collectionName = createCollection();
    TupleStream stream = getTupleStream(predefinedCollection);

    // #typeds-flow
    CompletionStage<UpdateResponse> copyCollection =
        SolrSource.fromTupleStream(stream)
            .map(tuple -> WriteMessage.createUpsertMessage(tupleToBook.apply(tuple)))
            .groupedWithin(5, Duration.ofMillis(10))
            .via(
                SolrFlow.typeds(
                    collectionName, SolrUpdateSettings.create(), bookToDoc, solrClient, Book.class))
            .runWith(Sink.ignore(), system)
            // explicit commit when stream ended
            .thenApply(
                done -> {
                  try {
                    return solrClient.commit(collectionName);
                  } catch (Exception e) {
                    throw new IllegalStateException(e);
                  }
                });
    // #typeds-flow

    resultOf(copyCollection);

    TupleStream stream2 = getTupleStream(collectionName);

    CompletionStage<List<String>> res2 =
        SolrSource.fromTupleStream(stream2)
            .map(t -> tupleToBook.apply(t).title)
            .runWith(Sink.seq(), system);

    List<String> result = new ArrayList<>(resultOf(res2));

    List<String> expect =
        Arrays.asList(
            "Akka Concurrency",
            "Akka in Action",
            "Effective Akka",
            "Learning Scala",
            "Programming in Scala",
            "Scala Puzzlers",
            "Scala for Spark in Production");

    assertEquals(expect, result);
  }

  @Test
  public void testKafkaExample() throws Exception {
    String collectionName = createCollection();

    List<CommittableMessage> messagesFromKafka =
        Arrays.asList(
            new CommittableMessage(new Book("Book 1"), new CommittableOffset(0)),
            new CommittableMessage(new Book("Book 2"), new CommittableOffset(1)),
            new CommittableMessage(new Book("Book 3"), new CommittableOffset(2)));

    Source<CommittableMessage, NotUsed> kafkaConsumerSource = Source.from(messagesFromKafka);
    // #kafka-example
    // Note: This code mimics Alpakka Kafka APIs
    CompletionStage<Done> completion =
        kafkaConsumerSource // Assume we get this from Kafka
            .map(
                kafkaMessage -> {
                  Book book = kafkaMessage.book;
                  // Transform message so that we can write to elastic
                  return WriteMessage.createUpsertMessage(book)
                      .withPassThrough(kafkaMessage.committableOffset);
                })
            .groupedWithin(5, Duration.ofMillis(10))
            .via(
                SolrFlow.typedsWithPassThrough(
                    collectionName,
                    // use implicit commits to Solr
                    SolrUpdateSettings.create().withCommitWithin(5),
                    bookToDoc,
                    solrClient,
                    Book.class))
            .map(
                messageResults ->
                    messageResults.stream()
                        .map(
                            result -> {
                              if (result.status() != 0) {
                                throw new RuntimeException("Failed to write message to Solr");
                              }
                              return result.passThrough();
                            })
                        .collect(Collectors.toList()))
            .map(ConsumerMessage::createCommittableOffsetBatch)
            .mapAsync(1, CommittableOffsetBatch::commitJavadsl)
            .runWith(Sink.ignore(), system);
    // #kafka-example

    resultOf(completion);

    // Make sure all messages was committed to kafka
    assertEquals(
        Arrays.asList(0, 1, 2),
        CommittableOffsetBatch.committedOffsets.stream()
            .map(o -> o.offset)
            .collect(Collectors.toList()));

    TupleStream stream = getTupleStream(collectionName);

    CompletionStage<List<String>> res2 =
        SolrSource.fromTupleStream(stream)
            .map(t -> tupleToBook.apply(t).title)
            .runWith(Sink.seq(), system);

    List<String> result = new ArrayList<>(resultOf(res2));

    assertEquals(
        messagesFromKafka.stream().map(m -> m.book.title).sorted().collect(Collectors.toList()),
        result.stream().sorted().collect(Collectors.toList()));
  }

  @Test
  public void deleteDocuments() throws Exception {
    String collectionName = createCollection();
    TupleStream stream = getTupleStream(predefinedCollection);

    CompletionStage<UpdateResponse> copyCollection =
        SolrSource.fromTupleStream(stream)
            .map(
                tuple -> {
                  Book book = tupleToBook.apply(tuple);
                  SolrInputDocument doc = bookToDoc.apply(book);
                  return WriteMessage.createUpsertMessage(doc);
                })
            .groupedWithin(5, Duration.ofMillis(10))
            .runWith(
                SolrSink.documents(collectionName, SolrUpdateSettings.create(), solrClient), system)
            // explicit commit when stream ended
            .thenApply(
                done -> {
                  try {
                    return solrClient.commit(collectionName);
                  } catch (Exception e) {
                    throw new IllegalStateException(e);
                  }
                });

    resultOf(copyCollection);

    TupleStream stream2 = getTupleStream(collectionName);

    // #delete-documents
    CompletionStage<UpdateResponse> deleteDocuments =
        SolrSource.fromTupleStream(stream2)
            .map(
                t -> {
                  String id = tupleToBook.apply(t).title;
                  return WriteMessage.<SolrInputDocument>createDeleteMessage(id);
                })
            .groupedWithin(5, Duration.ofMillis(10))
            .runWith(
                SolrSink.documents(collectionName, SolrUpdateSettings.create(), solrClient), system)
            // explicit commit when stream ended
            .thenApply(
                done -> {
                  try {
                    return solrClient.commit(collectionName);
                  } catch (Exception e) {
                    throw new IllegalStateException(e);
                  }
                });
    // #delete-documents

    resultOf(deleteDocuments);

    TupleStream stream3 = getTupleStream(collectionName);

    CompletionStage<List<String>> res3 =
        SolrSource.fromTupleStream(stream3)
            .map(t -> tupleToBook.apply(t).title)
            .runWith(Sink.seq(), system);

    List<String> result = new ArrayList<>(resultOf(res3));
    List<String> expect = Collections.emptyList();
    assertEquals(expect, result);
  }

  @Test
  public void atomicUpdateDocuments() throws Exception {
    String collectionName = createCollection();
    TupleStream stream = getTupleStream(predefinedCollection);

    CompletionStage<UpdateResponse> copyCollection =
        SolrSource.fromTupleStream(stream)
            .map(
                tuple -> {
                  Book book = new Book(tupleToBook.apply(tuple).title, "Written by good authors.");
                  SolrInputDocument doc = bookToDoc.apply(book);
                  return WriteMessage.createUpsertMessage(doc);
                })
            .groupedWithin(5, Duration.ofMillis(10))
            .runWith(
                SolrSink.documents(collectionName, SolrUpdateSettings.create(), solrClient), system)
            // explicit commit when stream ended
            .thenApply(
                done -> {
                  try {
                    return solrClient.commit(collectionName);
                  } catch (Exception e) {
                    throw new IllegalStateException(e);
                  }
                });

    resultOf(copyCollection);

    TupleStream stream2 = getTupleStream(collectionName);

    // #update-atomically-documents
    CompletionStage<UpdateResponse> updateCollection =
        SolrSource.fromTupleStream(stream2)
            .map(
                t -> {
                  String id = t.fields.get("title").toString();
                  String comment = t.fields.get("comment").toString();
                  Map<String, Object> m2 = new HashMap<>();
                  m2.put("set", (comment + " It's is a good book!!!"));
                  Map<String, Map<String, Object>> updates = new HashMap<>();
                  updates.put("comment", m2);
                  return WriteMessage.<SolrInputDocument>createUpdateMessage("title", id, updates);
                })
            .groupedWithin(5, Duration.ofMillis(10))
            .runWith(
                SolrSink.documents(collectionName, SolrUpdateSettings.create(), solrClient), system)
            // explicit commit when stream ended
            .thenApply(
                done -> {
                  try {
                    return solrClient.commit(collectionName);
                  } catch (Exception e) {
                    throw new IllegalStateException(e);
                  }
                });
    // #update-atomically-documents

    resultOf(updateCollection);

    TupleStream stream3 = getTupleStream(collectionName);
    CompletionStage<List<String>> res3 =
        SolrSource.fromTupleStream(stream3)
            .map(
                t -> {
                  Book b = tupleToBook.apply(t);
                  return b.title + ". " + b.comment;
                })
            .runWith(Sink.seq(), system);

    List<String> result = new ArrayList<>(resultOf(res3));
    List<String> expect =
        Arrays.asList(
            "Akka Concurrency. Written by good authors. It's is a good book!!!",
            "Akka in Action. Written by good authors. It's is a good book!!!",
            "Effective Akka. Written by good authors. It's is a good book!!!",
            "Learning Scala. Written by good authors. It's is a good book!!!",
            "Programming in Scala. Written by good authors. It's is a good book!!!",
            "Scala Puzzlers. Written by good authors. It's is a good book!!!",
            "Scala for Spark in Production. Written by good authors. It's is a good book!!!");
    assertEquals(expect, result);
  }

  @Test
  public void atomicUpdateDocumentsWithRouter() throws Exception {
    String collectionName = createCollection("router");
    TupleStream stream = getTupleStream(predefinedCollection);

    CompletionStage<UpdateResponse> copyCollection =
        SolrSource.fromTupleStream(stream)
            .map(
                tuple -> {
                  Book book =
                      new Book(
                          tupleToBook.apply(tuple).title,
                          "Written by good authors.",
                          "router-value");
                  SolrInputDocument doc = bookToDoc.apply(book);
                  return WriteMessage.createUpsertMessage(doc);
                })
            .groupedWithin(5, Duration.ofMillis(10))
            .runWith(
                SolrSink.documents(collectionName, SolrUpdateSettings.create(), solrClient), system)
            // explicit commit when stream ended
            .thenApply(
                done -> {
                  try {
                    return solrClient.commit(collectionName);
                  } catch (Exception e) {
                    throw new IllegalStateException(e);
                  }
                });

    resultOf(copyCollection);

    TupleStream stream2 = getTupleStream(collectionName);

    CompletionStage<UpdateResponse> updateCollection =
        SolrSource.fromTupleStream(stream2)
            .map(
                t -> {
                  String id = t.fields.get("title").toString();
                  String comment = t.fields.get("comment").toString();
                  Map<String, Object> m2 = new HashMap<>();
                  m2.put("set", (comment + " It's is a good book!!!"));
                  Map<String, Map<String, Object>> updates = new HashMap<>();
                  updates.put("comment", m2);
                  return WriteMessage.<SolrInputDocument>createUpdateMessage("title", id, updates)
                      .withRoutingFieldValue("router-value");
                })
            .groupedWithin(5, Duration.ofMillis(10))
            .runWith(
                SolrSink.documents(collectionName, SolrUpdateSettings.create(), solrClient), system)
            // explicit commit when stream ended
            .thenApply(
                done -> {
                  try {
                    return solrClient.commit(collectionName);
                  } catch (Exception e) {
                    throw new IllegalStateException(e);
                  }
                });

    resultOf(updateCollection);

    TupleStream stream3 = getTupleStream(collectionName);

    CompletionStage<List<String>> res3 =
        SolrSource.fromTupleStream(stream3)
            .map(
                t -> {
                  Book b = tupleToBook.apply(t);
                  return b.title + ". " + b.comment;
                })
            .runWith(Sink.seq(), system);

    List<String> result = new ArrayList<>(resultOf(res3));

    List<String> expect =
        Arrays.asList(
            "Akka Concurrency. Written by good authors. It's is a good book!!!",
            "Akka in Action. Written by good authors. It's is a good book!!!",
            "Effective Akka. Written by good authors. It's is a good book!!!",
            "Learning Scala. Written by good authors. It's is a good book!!!",
            "Programming in Scala. Written by good authors. It's is a good book!!!",
            "Scala Puzzlers. Written by good authors. It's is a good book!!!",
            "Scala for Spark in Production. Written by good authors. It's is a good book!!!");

    assertEquals(expect, result);
  }

  @Test
  public void deleteDocumentsByQuery() throws Exception {
    String collectionName = createCollection();
    TupleStream stream = getTupleStream(predefinedCollection);

    CompletionStage<UpdateResponse> copyCollection =
        SolrSource.fromTupleStream(stream)
            .map(
                tuple -> {
                  Book book = tupleToBook.apply(tuple);
                  SolrInputDocument doc = bookToDoc.apply(book);
                  return WriteMessage.createUpsertMessage(doc);
                })
            .groupedWithin(5, Duration.ofMillis(10))
            .runWith(
                SolrSink.documents(collectionName, SolrUpdateSettings.create(), solrClient), system)
            // explicit commit when stream ended
            .thenApply(
                done -> {
                  try {
                    return solrClient.commit(collectionName);
                  } catch (Exception e) {
                    throw new IllegalStateException(e);
                  }
                });

    resultOf(copyCollection);

    TupleStream stream2 = getTupleStream(collectionName);

    // #delete-documents-query
    CompletionStage<UpdateResponse> deleteByQuery =
        SolrSource.fromTupleStream(stream2)
            .map(
                t -> {
                  String id = t.fields.get("title").toString();
                  return WriteMessage.<SolrInputDocument>createDeleteByQueryMessage(
                      "title:\"" + id + "\"");
                })
            .groupedWithin(5, Duration.ofMillis(10))
            .runWith(
                SolrSink.documents(collectionName, SolrUpdateSettings.create(), solrClient), system)
            // explicit commit when stream ended
            .thenApply(
                done -> {
                  try {
                    return solrClient.commit(collectionName);
                  } catch (Exception e) {
                    throw new IllegalStateException(e);
                  }
                });
    // #delete-documents-query

    resultOf(deleteByQuery);

    TupleStream stream3 = getTupleStream(collectionName);

    CompletionStage<List<String>> res3 =
        SolrSource.fromTupleStream(stream3)
            .map(t -> tupleToBook.apply(t).title)
            .runWith(Sink.seq(), system);

    List<String> result = new ArrayList<>(resultOf(res3));
    List<String> expect = Collections.emptyList();
    assertEquals(expect, result);
  }

  @Test
  public void testKafkaExamplePT() throws Exception {
    String collectionName = createCollection();

    List<CommittableOffset> messagesFromKafka =
        Arrays.asList(new CommittableOffset(0), new CommittableOffset(1), new CommittableOffset(2));

    CommittableOffsetBatch.committedOffsets.clear();

    Source<CommittableOffset, NotUsed> kafkaConsumerSource = Source.from(messagesFromKafka);
    // #kafka-example-PT
    // Note: This code mimics Alpakka Kafka APIs
    CompletionStage<Done> completion =
        kafkaConsumerSource // Assume we get this from Kafka
            .map(
                kafkaMessage -> {
                  // Transform message so that we can write to elastic
                  return WriteMessage.createPassThrough(kafkaMessage)
                      .withSource(new SolrInputDocument());
                })
            .groupedWithin(5, Duration.ofMillis(10))
            .via(
                SolrFlow.documentsWithPassThrough(
                    collectionName,
                    // use implicit commits to Solr
                    SolrUpdateSettings.create().withCommitWithin(5),
                    solrClient))
            .map(
                messageResults ->
                    messageResults.stream()
                        .map(
                            result -> {
                              if (result.status() != 0) {
                                throw new RuntimeException("Failed to write message to Solr");
                              }
                              return result.passThrough();
                            })
                        .collect(Collectors.toList()))
            .map(ConsumerMessage::createCommittableOffsetBatch)
            .mapAsync(1, CommittableOffsetBatch::commitJavadsl)
            .runWith(Sink.ignore(), system);
    // #kafka-example-PT

    resultOf(completion);

    // Make sure all messages was committed to kafka
    assertEquals(
        Arrays.asList(0, 1, 2),
        CommittableOffsetBatch.committedOffsets.stream()
            .map(o -> o.offset)
            .collect(Collectors.toList()));
  }

  @BeforeClass
  public static void setup() throws Exception {
    setupCluster();

    // #solr-update-settings
    SolrUpdateSettings settings = SolrUpdateSettings.create().withCommitWithin(-1);
    // #solr-update-settings

    CollectionAdminRequest.createCollection(predefinedCollection, "conf", 1, 1).process(solrClient);
    new UpdateRequest()
        .add("title", "Akka in Action")
        .add("title", "Programming in Scala")
        .add("title", "Learning Scala")
        .add("title", "Scala for Spark in Production")
        .add("title", "Scala Puzzlers")
        .add("title", "Effective Akka")
        .add("title", "Akka Concurrency")
        .commit(solrClient, predefinedCollection);
  }

  @AfterClass
  public static void teardown() throws Exception {
    solrClient.close();
    cluster.shutdown();
    zkTestServer.shutdown();
    TestKit.shutdownActorSystem(system);
  }

  static class CommittableOffset {
    final int offset;

    public CommittableOffset(int offset) {
      this.offset = offset;
    }
  }

  static class CommittableOffsetBatch {
    static List<CommittableOffset> committedOffsets = new ArrayList<>();

    private final List<CommittableOffset> offsets;

    public CommittableOffsetBatch(List<CommittableOffset> offsets) {
      this.offsets = offsets;
    }

    CompletionStage<Done> commitJavadsl() {
      committedOffsets.addAll(offsets);
      return CompletableFuture.completedFuture(Done.getInstance());
    }
  }

  static class ConsumerMessage {

    static CommittableOffsetBatch createCommittableOffsetBatch(List<CommittableOffset> offsets) {
      return new CommittableOffsetBatch(offsets);
    }
  }

  static class CommittableMessage {
    final Book book;
    final CommittableOffset committableOffset;

    public CommittableMessage(Book book, CommittableOffset offset) {
      this.book = book;
      this.committableOffset = offset;
    }
  }

  private static void setupCluster() throws Exception {
    File targetDir = new File("solr/target");
    File testWorkingDir = new File(targetDir, "java-solr-" + System.currentTimeMillis());
    if (!testWorkingDir.isDirectory()) {
      boolean mkdirs = testWorkingDir.mkdirs();
    }

    File confDir = new File("solr/src/test/resources/conf");

    Path zkDir = testWorkingDir.toPath().resolve("zookeeper/server/data");
    zkTestServer = new ZkTestServer(zkDir, zookeeperPort);
    zkTestServer.run();

    cluster =
        new MiniSolrCloudCluster(
            1,
            testWorkingDir.toPath(),
            MiniSolrCloudCluster.DEFAULT_CLOUD_SOLR_XML,
            JettyConfig.builder().setContext("/solr").build(),
            zkTestServer);

    // #init-client

    CloudSolrClient solrClient =
        new CloudSolrClient.Builder(Arrays.asList(zookeeperHost), Optional.empty()).build();
    // #init-client
    SolrTest.solrClient = solrClient;

    ((ZkClientClusterStateProvider) solrClient.getClusterStateProvider())
        .uploadConfig(confDir.toPath(), "conf");

    solrClient.setIdField("router");

    assertTrue(!solrClient.getZkStateReader().getClusterState().getLiveNodes().isEmpty());
  }

  private static AtomicInteger number = new AtomicInteger(2);

  private static String createCollection() throws IOException, SolrServerException {
    String name = "collection-" + number.incrementAndGet();
    CollectionAdminRequest.createCollection(name, "conf", 1, 1).process(solrClient);
    return name;
  }

  private static String createCollection(String router) throws IOException, SolrServerException {
    String name = "collection-" + number.incrementAndGet();
    CollectionAdminRequest.createCollection(name, "conf", 1, 1)
        .setRouterField(router)
        .process(solrClient);
    return name;
  }

  private TupleStream getTupleStream(String collection) throws IOException {
    // #tuple-stream
    StreamFactory factory = new StreamFactory().withCollectionZkHost(collection, zookeeperHost);
    SolrClientCache solrClientCache = new SolrClientCache();
    StreamContext streamContext = new StreamContext();
    streamContext.setSolrClientCache(solrClientCache);

    String expressionStr =
        String.format("search(%s, q=*:*, fl=\"title,comment\", sort=\"title asc\")", collection);
    StreamExpression expression = StreamExpressionParser.parse(expressionStr);
    TupleStream stream = new CloudSolrStream(expression, factory);
    stream.setStreamContext(streamContext);

    Source<Tuple, NotUsed> source = SolrSource.fromTupleStream(stream);
    // #tuple-stream
    return stream;
  }

  /** Overwrite to set different default timeout for [[#resultOf]]. */
  protected Duration resultOfTimeout() {
    return Duration.ofSeconds(5);
  }

  protected <T> T resultOf(CompletionStage<T> stage) throws Exception {
    return resultOf(stage, resultOfTimeout());
  }

  protected <T> T resultOf(CompletionStage<T> stage, Duration timeout) throws Exception {
    return stage.toCompletableFuture().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
  }
}
