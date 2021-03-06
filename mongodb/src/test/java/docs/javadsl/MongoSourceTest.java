/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.Materializer;
import akka.stream.alpakka.mongodb.javadsl.MongoSource;
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.Sink;
import akka.stream.testkit.javadsl.StreamTestKit;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.reactivestreams.client.*;
import org.bson.Document;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.junit.*;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

public class MongoSourceTest {
  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  private static ActorSystem system;

  private final MongoClient client;
  private final MongoDatabase db;
  private final MongoCollection<Document> numbersDocumentColl;
  private final MongoCollection<Number> numbersColl;

  public MongoSourceTest() {
    // #init-system
    system = ActorSystem.create();
    // #init-system

    // #codecs
    PojoCodecProvider codecProvider = PojoCodecProvider.builder().register(Number.class).build();
    CodecRegistry codecRegistry =
        CodecRegistries.fromProviders(codecProvider, new ValueCodecProvider());
    // #codecs

    // #init-connection
    client = MongoClients.create("mongodb://localhost:27017");
    db = client.getDatabase("MongoSourceTest");
    numbersColl = db.getCollection("numbers", Number.class).withCodecRegistry(codecRegistry);
    // #init-connection

    numbersDocumentColl = db.getCollection("numbers");
  }

  @Before
  public void cleanDb() throws Exception {
    Source.fromPublisher(numbersDocumentColl.deleteMany(new Document()))
        .runWith(Sink.head(), system)
        .toCompletableFuture()
        .get(5, TimeUnit.SECONDS);
  }

  @After
  public void checkForLeaks() throws Exception {
    Source.fromPublisher(numbersDocumentColl.deleteMany(new Document()))
        .runWith(Sink.head(), system)
        .toCompletableFuture()
        .get(5, TimeUnit.SECONDS);
    StreamTestKit.assertAllStagesStopped(Materializer.matFromSystem(system));
  }

  @AfterClass
  public static void terminateActorSystem() {
    system.terminate();
  }

  @Test
  public void streamTheResultOfASimpleMongoQuery() throws Exception {
    List<Integer> data = seed();

    final Source<Document, NotUsed> source = MongoSource.create(numbersDocumentColl.find());
    final CompletionStage<List<Document>> rows = source.runWith(Sink.seq(), system);

    assertEquals(
        data,
        rows.toCompletableFuture().get(5, TimeUnit.SECONDS).stream()
            .map(n -> n.getInteger("_id"))
            .collect(Collectors.toList()));
  }

  @Test
  public void supportCodecRegistryToReadClassObjects() throws Exception {
    List<Number> data = seed().stream().map(Number::new).collect(Collectors.toList());

    // #create-source
    final Source<Number, NotUsed> source = MongoSource.create(numbersColl.find(Number.class));
    // #create-source

    // #run-source
    final CompletionStage<List<Number>> rows = source.runWith(Sink.seq(), system);
    // #run-source

    assertEquals(data, rows.toCompletableFuture().get(5, TimeUnit.SECONDS));
  }

  @Test
  public void supportMultipleMaterializations() throws Exception {
    final List<Integer> data = seed();

    final Source<Document, NotUsed> source = MongoSource.create(numbersDocumentColl.find());

    assertEquals(
        data,
        source.runWith(Sink.seq(), system).toCompletableFuture().get(5, TimeUnit.SECONDS).stream()
            .map(n -> n.getInteger("_id"))
            .collect(Collectors.toList()));
    assertEquals(
        data,
        source.runWith(Sink.seq(), system).toCompletableFuture().get(5, TimeUnit.SECONDS).stream()
            .map(n -> n.getInteger("_id"))
            .collect(Collectors.toList()));
  }

  @Test
  public void streamTheResultOfMongoQueryThatResultsInNoData() throws Exception {
    final Source<Document, NotUsed> source = MongoSource.create(numbersDocumentColl.find());

    assertEquals(
        true,
        source.runWith(Sink.seq(), system).toCompletableFuture().get(5, TimeUnit.SECONDS).isEmpty());
  }

  private List<Integer> seed() throws Exception {
    final List<Integer> numbers = IntStream.range(1, 10).boxed().collect(Collectors.toList());

    final List<Document> documents =
        numbers.stream().map(i -> Document.parse("{_id:" + i + "}")).collect(Collectors.toList());

    final CompletionStage<InsertManyResult> completion =
        Source.fromPublisher(numbersDocumentColl.insertMany(documents)).runWith(Sink.head(), system);
    completion.toCompletableFuture().get(5, TimeUnit.SECONDS);

    return numbers;
  }
}
