/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.alpakka.orientdb.OrientDbWriteMessage;
import akka.stream.alpakka.orientdb.OrientDbSourceSettings;
import akka.stream.alpakka.orientdb.OrientDbWriteSettings;
import akka.stream.alpakka.orientdb.javadsl.OrientDbFlow;
import akka.stream.alpakka.orientdb.javadsl.OrientDbSink;
import akka.stream.alpakka.orientdb.javadsl.OrientDbSource;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.JavaTestKit;
import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
// #init-settings
import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
// #init-settings
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class OrientDbTest {

  private static OServerAdmin oServerAdmin;
  private static OPartitionedDatabasePool oDatabase;
  private static ODatabaseDocumentTx client;
  private static ActorSystem system;
  private static ActorMaterializer materializer;

  // #init-settings

  private static String url = "remote:127.0.0.1:2424/";
  private static String dbName = "GratefulDeadConcertsJava";
  private static String dbUrl = url + dbName;
  private static String username = "root";
  private static String password = "root";
  // #init-settings

  private static String sourceClass = "source1";
  private static String sinkClass1 = "sink1";
  private static String sinkClass2 = "sink2";
  private static String sink3 = "sink3";
  private static String sink6 = "sink6";

  // #define-class
  public static class source1 {

    private String book_title;

    public void setBook_title(String book_title) {
      this.book_title = book_title;
    }

    public String getBook_title() {
      return book_title;
    }
  }

  public static class sink2 {

    private String book_title;

    public void setBook_title(String book_title) {
      this.book_title = book_title;
    }

    public String getBook_title() {
      return book_title;
    }
  }
  // #define-class

  public static class KafkaOffset {

    private int offset;

    public KafkaOffset(int offset) {
      this.offset = offset;
    }

    public void setOffset(int offset) {
      this.offset = offset;
    }

    public int getOffset() {
      return offset;
    }
  }

  public static class messagesFromKafka {

    private String book_title;

    private KafkaOffset kafkaOffset;

    public messagesFromKafka(String book_title, KafkaOffset kafkaOffset) {
      this.book_title = book_title;
      this.kafkaOffset = kafkaOffset;
    }

    public void setBook_title(String book_title) {
      this.book_title = book_title;
    }

    public String getBook_title() {
      return book_title;
    }

    public void setKafkaOffset(KafkaOffset kafkaOffset) {
      this.kafkaOffset = kafkaOffset;
    }

    public KafkaOffset getKafkaOffset() {
      return kafkaOffset;
    }
  }

  @BeforeClass
  public static void setup() throws Exception {
    system = ActorSystem.create();
    materializer = ActorMaterializer.create(system);

    oServerAdmin = new OServerAdmin(url).connect(username, password);
    if (!oServerAdmin.existsDatabase(dbName, "plocal")) {
      oServerAdmin.createDatabase(dbName, "document", "plocal");
    }

    // #init-settings

    oDatabase =
        new OPartitionedDatabasePool(
            dbUrl, username, password, Runtime.getRuntime().availableProcessors(), 10);

    system.registerOnTermination(() -> oDatabase.close());
    // #init-settings
    client = oDatabase.acquire();

    register(sourceClass);

    flush(sourceClass, "book_title", "Akka in Action");
    flush(sourceClass, "book_title", "Programming in Scala");
    flush(sourceClass, "book_title", "Learning Scala");
    flush(sourceClass, "book_title", "Scala for Spark in Production");
    flush(sourceClass, "book_title", "Scala Puzzlers");
    flush(sourceClass, "book_title", "Effective Akka");
    flush(sourceClass, "book_title", "Akka Concurrency");
  }

  @AfterClass
  public static void tearDown() throws Exception {
    unregister(sourceClass);
    unregister(sinkClass1);
    unregister(sinkClass2);
    unregister(sink3);
    unregister(sink6);

    if (oServerAdmin.existsDatabase(dbName, "plocal")) {
      oServerAdmin.dropDatabase(dbName, "plocal");
    }
    oServerAdmin.close();

    client.close();
    oDatabase.close();
    JavaTestKit.shutdownActorSystem(system);
  }

  private static void register(String className) {
    if (!client.getMetadata().getSchema().existsClass(className)) {
      client.getMetadata().getSchema().createClass(className);
    }
  }

  private static void flush(String className, String fieldName, String fieldValue) {
    ODocument oDocument = new ODocument().field(fieldName, fieldValue);
    oDocument.setClassNameIfExists(className);
    oDocument.save();
  }

  private static void unregister(String className) {
    if (client.getMetadata().getSchema().existsClass(className)) {
      client.getMetadata().getSchema().dropClass(className);
    }
  }

  @Test
  public void sourceSettings() {
    // #source-settings
    // re-iterating default values
    OrientDbSourceSettings sourceSettings =
        OrientDbSourceSettings.create(oDatabase).withSkip(0).withLimit(10);
    // #source-settings
  }

  @Test
  public void writeSettings() {
    // #write-settings
    // re-iterating default values
    OrientDbWriteSettings updateSettings = OrientDbWriteSettings.create(oDatabase);
    // #write-settings
  }

  @Test
  public void oDocObjectStream() throws Exception {
    // Copy sourceClass to sinkClass1 through ODocument stream
    CompletionStage<Done> f1 =
        OrientDbSource.create(sourceClass, OrientDbSourceSettings.create(oDatabase))
            .map(m -> OrientDbWriteMessage.create(m.oDocument()))
            .groupedWithin(10, Duration.ofMillis(10))
            .runWith(
                OrientDbSink.create(sinkClass1, OrientDbWriteSettings.create(oDatabase)),
                materializer);

    f1.toCompletableFuture().get(10, TimeUnit.SECONDS);

    // #run-odocument
    CompletionStage<List<String>> result =
        OrientDbSource.create(sinkClass1, OrientDbSourceSettings.create(oDatabase))
            .map(m -> m.oDocument().<String>field("book_title"))
            .runWith(Sink.seq(), materializer);
    // #run-odocument

    List<String> res = new ArrayList<>(result.toCompletableFuture().get(10, TimeUnit.SECONDS));

    List<String> expect =
        Arrays.asList(
            "Akka Concurrency",
            "Akka in Action",
            "Effective Akka",
            "Learning Scala",
            "Programming in Scala",
            "Scala Puzzlers",
            "Scala for Spark in Production");

    Collections.sort(res);
    assertEquals(expect, res);
  }

  @Test
  public void typedStream() throws Exception {
    // Copy sourceClass/book to sinkClass2/book through Typed stream
    // #run-typed
    CompletionStage<Done> f1 =
        OrientDbSource.typed(
                sourceClass, OrientDbSourceSettings.create(oDatabase), source1.class, null)
            .map(
                readResult -> {
                  ODatabaseDocumentTx db = oDatabase.acquire();
                  db.setDatabaseOwner(new OObjectDatabaseTx(db));
                  ODatabaseRecordThreadLocal.instance().set(db);
                  sink2 sink = new sink2();
                  sink.setBook_title(readResult.oDocument().getBook_title());
                  return OrientDbWriteMessage.create(sink);
                })
            .groupedWithin(10, Duration.ofMillis(10))
            .runWith(
                OrientDbSink.typed(
                    sinkClass2, OrientDbWriteSettings.create(oDatabase), sink2.class),
                materializer);
    // #run-typed

    f1.toCompletableFuture().get(10, TimeUnit.SECONDS);

    CompletionStage<List<String>> f2 =
        OrientDbSource.typed(
                sinkClass2, OrientDbSourceSettings.create(oDatabase), sink2.class, null)
            .map(
                m -> {
                  ODatabaseDocumentTx db = oDatabase.acquire();
                  db.setDatabaseOwner(new OObjectDatabaseTx(db));
                  ODatabaseRecordThreadLocal.instance().set(db);
                  return m.oDocument().getBook_title();
                })
            .runWith(Sink.seq(), materializer);

    List<String> result = new ArrayList<>(f2.toCompletableFuture().get(10, TimeUnit.SECONDS));

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
  public void typedStreamWithPassThrough() throws Exception {
    // #kafka-example
    // We're going to pretend we got messages from kafka.
    // After we've written them to OrientDB, we want
    // to commit the offset to Kafka

    List<Integer> committedOffsets = new ArrayList<>();
    List<messagesFromKafka> messagesFromKafkas =
        Arrays.asList(
            new messagesFromKafka("Akka Concurrency", new KafkaOffset(0)),
            new messagesFromKafka("Akka in Action", new KafkaOffset(1)),
            new messagesFromKafka("Effective Akka", new KafkaOffset(2)));

    Consumer<KafkaOffset> commitToKafka =
        new Consumer<KafkaOffset>() {
          @Override
          public void accept(KafkaOffset kafkaOffset) {
            committedOffsets.add(kafkaOffset.getOffset());
          }
        };

    Source.from(messagesFromKafkas)
        .map(
            kafkaMessage -> {
              String book_title = kafkaMessage.getBook_title();
              return OrientDbWriteMessage.create(
                  new ODocument().field("book_title", book_title), kafkaMessage.kafkaOffset);
            })
        .groupedWithin(10, Duration.ofMillis(10))
        .via(OrientDbFlow.createWithPassThrough(sink6, OrientDbWriteSettings.create(oDatabase)))
        .map(
            messages -> {
              ODatabaseDocumentTx db = oDatabase.acquire();
              db.setDatabaseOwner(new OObjectDatabaseTx(db));
              ODatabaseRecordThreadLocal.instance().set(db);
              messages
                  .stream()
                  .forEach(
                      message -> {
                        commitToKafka.accept(((KafkaOffset) message.passThrough()));
                      });
              return NotUsed.getInstance();
            })
        .runWith(Sink.seq(), materializer)
        .toCompletableFuture()
        .get(10, TimeUnit.SECONDS);
    // #kafka-example

    assertEquals(Arrays.asList(0, 1, 2), committedOffsets);

    List<Object> result2 =
        OrientDbSource.create(sink6, OrientDbSourceSettings.create(oDatabase), null)
            .map(m -> m.oDocument().field("book_title"))
            .runWith(Sink.seq(), materializer)
            .toCompletableFuture()
            .get(10, TimeUnit.SECONDS);

    assertEquals(
        messagesFromKafkas
            .stream()
            .map(m -> m.getBook_title())
            .sorted()
            .collect(Collectors.toList()),
        result2.stream().sorted().collect(Collectors.toList()));
  }

  @Test
  public void flow() throws Exception {
    // Copy sourceClass to sink3 through ODocument stream
    // #run-flow
    CompletionStage<List<List<OrientDbWriteMessage<ODocument, NotUsed>>>> f1 =
        OrientDbSource.create(sourceClass, OrientDbSourceSettings.create(oDatabase), null)
            .map(m -> OrientDbWriteMessage.create(m.oDocument()))
            .groupedWithin(10, Duration.ofMillis(10))
            .via(OrientDbFlow.create(sink3, OrientDbWriteSettings.create(oDatabase)))
            .runWith(Sink.seq(), materializer);
    // #run-flow

    f1.toCompletableFuture().get(10, TimeUnit.SECONDS);

    // Assert docs in sink3
    CompletionStage<List<String>> f2 =
        OrientDbSource.create(sink3, OrientDbSourceSettings.create(oDatabase), null)
            .map(m -> m.oDocument().<String>field("book_title"))
            .runWith(Sink.seq(), materializer);

    List<String> result2 = new ArrayList<>(f2.toCompletableFuture().get(10, TimeUnit.SECONDS));

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
}
