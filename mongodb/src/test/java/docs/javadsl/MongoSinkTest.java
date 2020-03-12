/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.mongodb.DocumentReplace;
import akka.stream.alpakka.mongodb.DocumentUpdate;
import akka.stream.alpakka.mongodb.javadsl.MongoSink;
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
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
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.junit.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MongoSinkTest {
  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  private static ActorSystem system;
  private static Materializer mat;

  private final MongoClient client;
  private final MongoDatabase db;
  private final MongoCollection<Document> numbersDocumentColl;
  private final MongoCollection<Number> numbersColl;
  private final MongoCollection<Document> domainObjectsDocumentColl;
  private final MongoCollection<DomainObject> domainObjectsColl;

  private final List<Integer> testRange = IntStream.range(1, 10).boxed().collect(toList());

  public MongoSinkTest() {
    system = ActorSystem.create();
    mat = ActorMaterializer.create(system);

    PojoCodecProvider codecProvider =
        PojoCodecProvider.builder().register(Number.class, DomainObject.class).build();
    CodecRegistry codecRegistry =
        fromRegistries(fromProviders(codecProvider, new ValueCodecProvider()));

    client = MongoClients.create("mongodb://localhost:27017");
    db = client.getDatabase("MongoSinkTest");
    numbersColl = db.getCollection("numbers", Number.class).withCodecRegistry(codecRegistry);
    numbersDocumentColl = db.getCollection("numbers");
    domainObjectsColl =
        db.getCollection("domainObjects", DomainObject.class).withCodecRegistry(codecRegistry);
    domainObjectsDocumentColl = db.getCollection("domainObjects");
  }

  private void insertTestRange() throws Exception {
    Source.from(testRange)
        .map(i -> Document.parse("{\"value\":" + i + "}"))
        .runWith(MongoSink.insertOne(numbersDocumentColl), mat)
        .toCompletableFuture()
        .get(5, TimeUnit.SECONDS);
  }

  private void insertDomainObjects() throws Exception {
    Source.from(testRange)
        .map(
            i -> {
              DomainObject domainObject =
                  new DomainObject(
                      i,
                      String.format("first-property-%s", i),
                      String.format("second-property-%s", i));
              System.out.println(String.format("%s inserting %s", i, domainObject));
              return domainObject;
            })
        .runWith(MongoSink.insertOne(domainObjectsColl), mat)
        .toCompletableFuture()
        .get(5, TimeUnit.SECONDS);
  }

  private void deleteCollection(MongoCollection<Document> collection) throws Exception {
    Source.fromPublisher(collection.deleteMany(new Document()))
        .runWith(Sink.head(), mat)
        .toCompletableFuture()
        .get(5, TimeUnit.SECONDS);
  }

  private void checkCollectionForLeaks(MongoCollection<Document> collection) throws Exception {
    Source.fromPublisher(collection.deleteMany(new Document()))
        .runWith(Sink.head(), mat)
        .toCompletableFuture()
        .get(5, TimeUnit.SECONDS);
  }

  @Before
  public void cleanDb() throws Exception {
    deleteCollection(numbersDocumentColl);
    deleteCollection(domainObjectsDocumentColl);
  }

  @After
  public void checkForLeaks() throws Exception {
    checkCollectionForLeaks(numbersDocumentColl);
    checkCollectionForLeaks(domainObjectsDocumentColl);
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
            .collect(toList()));
  }

  @Test
  public void saveWithInsertOneAndCodecSupport() throws Exception {
    // #insert-one
    List<Number> testRangeObjects = testRange.stream().map(Number::new).collect(toList());
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
            .collect(toList()));
  }

  @Test
  public void saveWithInsertManyAndCodecSupport() throws Exception {
    // #insert-many
    final List<Number> testRangeObjects = testRange.stream().map(Number::new).collect(toList());
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
            .collect(toList()));
  }

  @Test
  public void saveWithInsertManyWithOptionsAndCodecSupport() throws Exception {
    List<Number> testRangeObjects = testRange.stream().map(Number::new).collect(toList());
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
        testRange.stream().map(i -> Pair.create(i, i * -1)).collect(toList()),
        found.toCompletableFuture().get(5, TimeUnit.SECONDS).stream()
            .map(d -> Pair.create(d.getInteger("value"), d.getInteger("updateValue")))
            .collect(toList()));
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
        testRange.stream().map(i -> Pair.create(i, 0)).collect(toList()),
        found.toCompletableFuture().get(5, TimeUnit.SECONDS).stream()
            .map(d -> Pair.create(d.getInteger("value"), d.getInteger("updateValue")))
            .collect(toList()));
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

    assertTrue(found.toCompletableFuture().get(5, TimeUnit.SECONDS).isEmpty());
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

    assertTrue(found.toCompletableFuture().get(5, TimeUnit.SECONDS).isEmpty());
  }

  @Test
  public void replaceWithReplaceOne() throws Exception {
    insertDomainObjects();

    // #replace-one
    final Source<DocumentReplace<DomainObject>, NotUsed> source =
        Source.from(testRange)
            .map(
                i ->
                    DocumentReplace.create(
                        Filters.eq("_id", i),
                        new DomainObject(
                            i,
                            String.format("updated-first-property-%s", i),
                            String.format("updated-second-property-%s", i))));
    final CompletionStage<Done> completion =
        source.runWith(MongoSink.replaceOne(domainObjectsColl), mat);
    // #replace-one

    completion.toCompletableFuture().get(5, TimeUnit.SECONDS);

    final List<DomainObject> found =
        new ArrayList<>(
            Source.fromPublisher(domainObjectsColl.find())
                .runWith(Sink.seq(), mat)
                .toCompletableFuture()
                .get(5, TimeUnit.SECONDS));

    final List<DomainObject> expected =
        testRange.stream()
            .map(
                i ->
                    new DomainObject(
                        i,
                        String.format("updated-first-property-%s", i),
                        String.format("updated-second-property-%s", i)))
            .collect(toList());

    assertEquals(expected, found);
  }
}
