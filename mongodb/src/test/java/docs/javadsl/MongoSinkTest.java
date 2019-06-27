/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.mongodb.DocumentUpdate;
import akka.stream.alpakka.mongodb.javadsl.MongoSink;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.testkit.javadsl.StreamTestKit;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import org.bson.Document;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

public class MongoSinkTest {

  private static ActorSystem system;
  private static Materializer mat;

  private final MongoClient client;
  private final MongoDatabase db;
  private final MongoCollection<Document> numbersDocumentColl;
  private final MongoCollection<Number> numbersColl;

  private final List<Integer> testRange =
      IntStream.range(1, 10).boxed().collect(Collectors.toList());

  public MongoSinkTest() {
    system = ActorSystem.create();
    mat = ActorMaterializer.create(system);

    PojoCodecProvider codecProvider = PojoCodecProvider.builder().register(Number.class).build();
    CodecRegistry codecRegistry =
        CodecRegistries.fromProviders(codecProvider, new ValueCodecProvider());

    client = MongoClients.create("mongodb://localhost:27017");
    db = client.getDatabase("MongoSinkTest");
    numbersColl = db.getCollection("numbers", Number.class).withCodecRegistry(codecRegistry);
    numbersDocumentColl = db.getCollection("numbers");
  }

  private void insertTestRange() throws Exception {
    Source.from(testRange)
        .map(i -> Document.parse("{\"value\":" + i + "}"))
        .runWith(MongoSink.insertOne(numbersDocumentColl), mat)
        .toCompletableFuture()
        .get(5, TimeUnit.SECONDS);
  }

  @Before
  public void cleanDb() throws Exception {
    Source.fromPublisher(numbersDocumentColl.deleteMany(new Document()))
        .runWith(Sink.head(), mat)
        .toCompletableFuture()
        .get(5, TimeUnit.SECONDS);
  }

  @After
  public void checkForLeaks() throws Exception {
    Source.fromPublisher(numbersDocumentColl.deleteMany(new Document()))
        .runWith(Sink.head(), mat)
        .toCompletableFuture()
        .get(5, TimeUnit.SECONDS);
    StreamTestKit.assertAllStagesStopped(mat);
  }

  @AfterClass
  public static void terminateActorSystem() {
    system.terminate();
  }

  @Test
  public void saveWithInsertOne() throws Exception {
    final Source<Document, NotUsed> source =
        Source.from(testRange).map(i -> Document.parse("{\"value\":" + i + "}"));
    final CompletionStage<Done> completion =
        source.runWith(MongoSink.insertOne(numbersDocumentColl), mat);

    completion.toCompletableFuture().get(5, TimeUnit.SECONDS);
    final CompletionStage<List<Document>> found =
        Source.fromPublisher(numbersDocumentColl.find()).runWith(Sink.seq(), mat);

    assertEquals(
        testRange,
        found.toCompletableFuture().get(5, TimeUnit.SECONDS).stream()
            .map(n -> n.getInteger("value"))
            .collect(Collectors.toList()));
  }

  @Test
  public void saveWithInsertOneAndCodecSupport() throws Exception {
    // #insert-one
    List<Number> testRangeObjects =
        testRange.stream().map(Number::new).collect(Collectors.toList());
    final CompletionStage<Done> completion =
        Source.from(testRangeObjects).runWith(MongoSink.insertOne(numbersColl), mat);
    // #insert-one

    completion.toCompletableFuture().get(5, TimeUnit.SECONDS);
    final CompletionStage<List<Number>> found =
        Source.fromPublisher(numbersColl.find()).runWith(Sink.seq(), mat);

    assertEquals(testRangeObjects, found.toCompletableFuture().get(5, TimeUnit.SECONDS));
  }

  @Test
  public void saveWithInsertMany() throws Exception {
    final Source<Document, NotUsed> source =
        Source.from(testRange).map(i -> Document.parse("{\"value\":" + i + "}"));
    final CompletionStage<Done> completion =
        source.grouped(2).runWith(MongoSink.insertMany(numbersDocumentColl), mat);

    completion.toCompletableFuture().get(5, TimeUnit.SECONDS);
    final CompletionStage<List<Document>> found =
        Source.fromPublisher(numbersDocumentColl.find()).runWith(Sink.seq(), mat);

    assertEquals(
        testRange,
        found.toCompletableFuture().get(5, TimeUnit.SECONDS).stream()
            .map(n -> n.getInteger("value"))
            .collect(Collectors.toList()));
  }

  @Test
  public void saveWithInsertManyAndCodecSupport() throws Exception {
    // #insert-many
    final List<Number> testRangeObjects =
        testRange.stream().map(Number::new).collect(Collectors.toList());
    final CompletionStage<Done> completion =
        Source.from(testRangeObjects).grouped(2).runWith(MongoSink.insertMany(numbersColl), mat);
    // #insert-many

    completion.toCompletableFuture().get(5, TimeUnit.SECONDS);
    final CompletionStage<List<Number>> found =
        Source.fromPublisher(numbersColl.find()).runWith(Sink.seq(), mat);

    assertEquals(testRangeObjects, found.toCompletableFuture().get(5, TimeUnit.SECONDS));
  }

  @Test
  public void saveWithInsertManyWithOptions() throws Exception {
    final Source<Document, NotUsed> source =
        Source.from(testRange).map(i -> Document.parse("{\"value\":" + i + "}"));
    final CompletionStage<Done> completion =
        source
            .grouped(2)
            .runWith(
                MongoSink.insertMany(numbersDocumentColl, new InsertManyOptions().ordered(false)),
                mat);

    completion.toCompletableFuture().get(5, TimeUnit.SECONDS);
    final CompletionStage<List<Document>> found =
        Source.fromPublisher(numbersDocumentColl.find()).runWith(Sink.seq(), mat);

    assertEquals(
        testRange,
        found.toCompletableFuture().get(5, TimeUnit.SECONDS).stream()
            .map(n -> n.getInteger("value"))
            .collect(Collectors.toList()));
  }

  @Test
  public void saveWithInsertManyWithOptionsAndCodecSupport() throws Exception {
    List<Number> testRangeObjects =
        testRange.stream().map(Number::new).collect(Collectors.toList());
    final CompletionStage<Done> completion =
        Source.from(testRangeObjects)
            .grouped(2)
            .runWith(
                MongoSink.insertMany(numbersColl, new InsertManyOptions().ordered(false)), mat);

    completion.toCompletableFuture().get(5, TimeUnit.SECONDS);
    final CompletionStage<List<Number>> found =
        Source.fromPublisher(numbersColl.find()).runWith(Sink.seq(), mat);

    assertEquals(testRangeObjects, found.toCompletableFuture().get(5, TimeUnit.SECONDS));
  }

  @Test
  public void updateWithUpdateOne() throws Exception {
    insertTestRange();

    // #update-one
    final Source<DocumentUpdate, NotUsed> source =
        Source.from(testRange)
            .map(
                i ->
                    DocumentUpdate.create(
                        Filters.eq("value", i), Updates.set("updateValue", i * -1)));
    final CompletionStage<Done> completion =
        source.runWith(MongoSink.updateOne(numbersDocumentColl), mat);
    // #update-one

    completion.toCompletableFuture().get(5, TimeUnit.SECONDS);

    final CompletionStage<List<Document>> found =
        Source.fromPublisher(numbersDocumentColl.find()).runWith(Sink.seq(), mat);

    assertEquals(
        testRange.stream().map(i -> Pair.create(i, i * -1)).collect(Collectors.toList()),
        found.toCompletableFuture().get(5, TimeUnit.SECONDS).stream()
            .map(d -> Pair.create(d.getInteger("value"), d.getInteger("updateValue")))
            .collect(Collectors.toList()));
  }

  @Test
  public void updateWithUpdateMany() throws Exception {
    insertTestRange();

    final Source<DocumentUpdate, NotUsed> source =
        Source.single(0)
            .map(
                i -> DocumentUpdate.create(Filters.gte("value", 0), Updates.set("updateValue", 0)));
    final CompletionStage<Done> completion =
        source.runWith(MongoSink.updateMany(numbersDocumentColl), mat);

    completion.toCompletableFuture().get(5, TimeUnit.SECONDS);

    final CompletionStage<List<Document>> found =
        Source.fromPublisher(numbersDocumentColl.find()).runWith(Sink.seq(), mat);

    assertEquals(
        testRange.stream().map(i -> Pair.create(i, 0)).collect(Collectors.toList()),
        found.toCompletableFuture().get(5, TimeUnit.SECONDS).stream()
            .map(d -> Pair.create(d.getInteger("value"), d.getInteger("updateValue")))
            .collect(Collectors.toList()));
  }

  @Test
  public void deleteWithDeleteOne() throws Exception {
    insertTestRange();

    // #delete-one
    final Source<Bson, NotUsed> source = Source.from(testRange).map(i -> Filters.eq("value", i));
    final CompletionStage<Done> completion =
        source.runWith(MongoSink.deleteOne(numbersDocumentColl), mat);
    // #delete-one

    completion.toCompletableFuture().get(5, TimeUnit.SECONDS);

    final CompletionStage<List<Document>> found =
        Source.fromPublisher(numbersDocumentColl.find()).runWith(Sink.seq(), mat);

    assertEquals(true, found.toCompletableFuture().get(5, TimeUnit.SECONDS).isEmpty());
  }

  @Test
  public void deleteWithDeleteMany() throws Exception {
    insertTestRange();

    final Source<Bson, NotUsed> source = Source.single(0).map(i -> Filters.gte("value", 0));
    final CompletionStage<Done> completion =
        source.runWith(MongoSink.deleteMany(numbersDocumentColl), mat);

    completion.toCompletableFuture().get(5, TimeUnit.SECONDS);

    final CompletionStage<List<Document>> found =
        Source.fromPublisher(numbersDocumentColl.find()).runWith(Sink.seq(), mat);

    assertEquals(true, found.toCompletableFuture().get(5, TimeUnit.SECONDS).isEmpty());
  }
}
