/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
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
  private final MongoCollection<Document> numbersColl;
  private final MongoCollection<Number> numbersObjectColl;

  private final List<Integer> testRange = IntStream.range(1, 10)
    .boxed()
    .collect(Collectors.toList());

  public MongoSinkTest() {
    // #init-mat
    system = ActorSystem.create();
    mat = ActorMaterializer.create(system);
    // #init-mat

    // #init-connection
    client = MongoClients.create("mongodb://localhost:27017");
    db = client.getDatabase("MongoSinkTest");
    numbersColl = db.getCollection("numbers");
    // #init-connection

    PojoCodecProvider codecProvider = PojoCodecProvider.builder().register(Number.class).build();
    CodecRegistry codecRegistry = CodecRegistries.fromProviders(codecProvider, new ValueCodecProvider());

    // #init-connection-codec
    numbersObjectColl = db.getCollection("numbers", Number.class).withCodecRegistry(codecRegistry);
    // #init-connection-codec
  }

  private void insertTestRange() throws Exception {
    Source.from(testRange)
      .map(i -> Document.parse("{\"value\":" + i + "}"))
      .runWith(MongoSink.insertOne(numbersColl), mat)
      .toCompletableFuture().get(5, TimeUnit.SECONDS);
  }

  @Before
  public void cleanDb() throws Exception {
    Source.fromPublisher(numbersColl.deleteMany(new Document())).runWith(Sink.head(), mat).toCompletableFuture().get(5, TimeUnit.SECONDS);
  }

  @After
  public void checkForLeaks() throws Exception {
    Source.fromPublisher(numbersColl.deleteMany(new Document())).runWith(Sink.head(), mat).toCompletableFuture().get(5, TimeUnit.SECONDS);
    StreamTestKit.assertAllStagesStopped(mat);
  }

  @AfterClass
  public static void terminateActorSystem() {
    system.terminate();
  }

  @Test
  public void saveWithInsertOne() throws Exception {
    // #insertOne
    Source<Document, NotUsed> source = Source.from(testRange).map(i ->
      Document.parse("{\"value\":" + i + "}"));
    CompletionStage<Done> completion = source.runWith(MongoSink.insertOne(numbersColl), mat);
    // #insertOne

    completion.toCompletableFuture().get(5, TimeUnit.SECONDS);
    CompletionStage<List<Document>> found = Source.fromPublisher(numbersColl.find()).runWith(Sink.seq(), mat);

    assertEquals(testRange, found.toCompletableFuture().get(5, TimeUnit.SECONDS).stream().map(n -> n.getInteger("value")).collect(Collectors.toList()));
  }

  @Test
  public void saveWithInsertOneAndCodecSupport() throws Exception {
    List<Number> testRangeObjects = testRange.stream().map(Number::new).collect(Collectors.toList());
    CompletionStage<Done> completion = Source.from(testRangeObjects).runWith(MongoSink.insertOne(numbersObjectColl), mat);

    completion.toCompletableFuture().get(5, TimeUnit.SECONDS);
    CompletionStage<List<Number>> found = Source.fromPublisher(numbersObjectColl.find()).runWith(Sink.seq(), mat);

    assertEquals(testRangeObjects, found.toCompletableFuture().get(5, TimeUnit.SECONDS));
  }

  @Test
  public void saveWithInsertMany() throws Exception {
    // #insertMany
    Source<Document, NotUsed> source = Source.from(testRange).map(i ->
      Document.parse("{\"value\":" + i + "}"));
    CompletionStage<Done> completion = source.grouped(2).runWith(MongoSink.insertMany(numbersColl), mat);
    // #insertMany

    completion.toCompletableFuture().get(5, TimeUnit.SECONDS);
    CompletionStage<List<Document>> found = Source.fromPublisher(numbersColl.find()).runWith(Sink.seq(), mat);

    assertEquals(testRange, found.toCompletableFuture().get(5, TimeUnit.SECONDS).stream().map(n -> n.getInteger("value")).collect(Collectors.toList()));
  }

  @Test
  public void saveWithInsertManyAndCodecSupport() throws Exception {
    List<Number> testRangeObjects = testRange.stream().map(Number::new).collect(Collectors.toList());
    CompletionStage<Done> completion = Source.from(testRangeObjects).grouped(2).runWith(MongoSink.insertMany(numbersObjectColl), mat);

    completion.toCompletableFuture().get(5, TimeUnit.SECONDS);
    CompletionStage<List<Number>> found = Source.fromPublisher(numbersObjectColl.find()).runWith(Sink.seq(), mat);

    assertEquals(testRangeObjects, found.toCompletableFuture().get(5, TimeUnit.SECONDS));
  }

  @Test
  public void saveWithInsertManyWithOptions() throws Exception {
    // #insertMany
    Source<Document, NotUsed> source = Source.from(testRange).map(i ->
      Document.parse("{\"value\":" + i + "}"));
    CompletionStage<Done> completion = source.grouped(2).runWith(MongoSink.insertMany(numbersColl, new InsertManyOptions().ordered(false)), mat);
    // #insertMany

    completion.toCompletableFuture().get(5, TimeUnit.SECONDS);
    CompletionStage<List<Document>> found = Source.fromPublisher(numbersColl.find()).runWith(Sink.seq(), mat);

    assertEquals(testRange, found.toCompletableFuture().get(5, TimeUnit.SECONDS).stream().map(n -> n.getInteger("value")).collect(Collectors.toList()));
  }

  @Test
  public void saveWithInsertManyWithOptionsAndCodecSupport() throws Exception {
    List<Number> testRangeObjects = testRange.stream().map(Number::new).collect(Collectors.toList());
    CompletionStage<Done> completion = Source.from(testRangeObjects).grouped(2).runWith(MongoSink.insertMany(numbersObjectColl, new InsertManyOptions().ordered(false)), mat);

    completion.toCompletableFuture().get(5, TimeUnit.SECONDS);
    CompletionStage<List<Number>> found = Source.fromPublisher(numbersObjectColl.find()).runWith(Sink.seq(), mat);

    assertEquals(testRangeObjects, found.toCompletableFuture().get(5, TimeUnit.SECONDS));
  }

  @Test
  public void updateWithUpdateOne() throws Exception {
    insertTestRange();

    // #updateOne
    Source<DocumentUpdate, NotUsed> source = Source.from(testRange).map(i ->
      DocumentUpdate.create(Filters.eq("value", i), Updates.set("updateValue", i * -1)));
    CompletionStage<Done> completion = source.runWith(MongoSink.updateOne(numbersColl), mat);
    // #updateOne

    completion.toCompletableFuture().get(5, TimeUnit.SECONDS);

    CompletionStage<List<Document>> found = Source.fromPublisher(numbersColl.find()).runWith(Sink.seq(), mat);

    assertEquals(
      testRange.stream().map(i -> Pair.create(i, i * -1)).collect(Collectors.toList()),
      found.toCompletableFuture().get(5, TimeUnit.SECONDS).stream().map(d -> Pair.create(d.getInteger("value"), d.getInteger("updateValue"))).collect(Collectors.toList()));
  }

  @Test
  public void updateWithUpdateMany() throws Exception {
    insertTestRange();

    Source<DocumentUpdate, NotUsed> source = Source.single(0).map(i ->
      DocumentUpdate.create(Filters.gte("value", 0), Updates.set("updateValue", 0)));
    CompletionStage<Done> completion = source.runWith(MongoSink.updateMany(numbersColl), mat);

    completion.toCompletableFuture().get(5, TimeUnit.SECONDS);

    CompletionStage<List<Document>> found = Source.fromPublisher(numbersColl.find()).runWith(Sink.seq(), mat);

    assertEquals(
      testRange.stream().map(i -> Pair.create(i, 0)).collect(Collectors.toList()),
      found.toCompletableFuture().get(5, TimeUnit.SECONDS).stream().map(d -> Pair.create(d.getInteger("value"), d.getInteger("updateValue"))).collect(Collectors.toList()));
  }

  @Test
  public void deleteWithDeleteOne() throws Exception {
    insertTestRange();

    // #deleteOne
    Source<Bson, NotUsed> source = Source.from(testRange).map(i -> Filters.eq("value", i));
    CompletionStage<Done> completion = source.runWith(MongoSink.deleteOne(numbersColl), mat);
    // #deleteOne

    completion.toCompletableFuture().get(5, TimeUnit.SECONDS);

    CompletionStage<List<Document>> found = Source.fromPublisher(numbersColl.find()).runWith(Sink.seq(), mat);

    assertEquals(true, found.toCompletableFuture().get(5, TimeUnit.SECONDS).isEmpty());
  }

  @Test
  public void deleteWithDeleteMany() throws Exception {
    insertTestRange();

    Source<Bson, NotUsed> source = Source.single(0).map(i -> Filters.gte("value", 0));
    CompletionStage<Done> completion = source.runWith(MongoSink.deleteMany(numbersColl), mat);

    completion.toCompletableFuture().get(5, TimeUnit.SECONDS);

    CompletionStage<List<Document>> found = Source.fromPublisher(numbersColl.find()).runWith(Sink.seq(), mat);

    assertEquals(true, found.toCompletableFuture().get(5, TimeUnit.SECONDS).isEmpty());
  }
}
