/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.function.Function2;
import akka.japi.pf.PFBuilder;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.slick.javadsl.Slick;
import akka.stream.alpakka.slick.javadsl.SlickRow;
import akka.stream.alpakka.slick.javadsl.SlickSession;
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/**
 * This unit test is run using a local H2 database using `/tmp/alpakka-slick-h2-test` for temporary
 * storage.
 */
public class SlickTest {
  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  private static ActorSystem system;
  private static Materializer materializer;

  // #init-session
  private static final SlickSession session = SlickSession.forConfig("slick-h2");
  // #init-session

  private static final Set<User> users =
      IntStream.range(0, 40)
          .boxed()
          .map((i) -> new User(i, "Name" + i))
          .collect(Collectors.toSet());
  private static final Source<User, NotUsed> usersSource = Source.from(users);

  private static final Function<User, String> insertUser =
      (user) ->
          "INSERT INTO ALPAKKA_SLICK_JAVADSL_TEST_USERS VALUES ("
              + user.id
              + ", '"
              + user.name
              + "')";

  private static final Function2<User, Connection, PreparedStatement> insertUserPS =
      (user, connection) -> {
        PreparedStatement statement =
            connection.prepareStatement(
                "INSERT INTO ALPAKKA_SLICK_JAVADSL_TEST_USERS VALUES (?, ?)");
        statement.setInt(1, user.id);
        statement.setString(2, user.name);
        return statement;
      };

  private static final String selectAllUsers =
      "SELECT ID, NAME FROM ALPAKKA_SLICK_JAVADSL_TEST_USERS";

  @BeforeClass
  public static void setup() {
    // #init-mat
    system = ActorSystem.create();
    materializer = ActorMaterializer.create(system);
    // #init-mat

    executeStatement(
        "CREATE TABLE ALPAKKA_SLICK_JAVADSL_TEST_USERS(ID INTEGER, NAME VARCHAR(50))",
        session,
        materializer);
  }

  @After
  public void cleanUp() {
    executeStatement("DELETE FROM ALPAKKA_SLICK_JAVADSL_TEST_USERS", session, materializer);
  }

  @AfterClass
  public static void teardown() {
    executeStatement("DROP TABLE ALPAKKA_SLICK_JAVADSL_TEST_USERS", session, materializer);

    // #close-session
    system.registerOnTermination(session::close);
    // #close-session

    TestKit.shutdownActorSystem(system);
  }

  @Test
  public void testSinkPSThatThrowException() {
    final Sink<User, CompletionStage<Done>> slickSink =
        Slick.sink(
            session,
            (__, connection) ->
                connection.prepareStatement(
                    "INSERT INTO ALPAKKA_SLICK_JAVADSL_TEST_USERS VALUES (?, ?)"));
    assertThrows(
        ExecutionException.class,
        () ->
            usersSource
                .runWith(slickSink, materializer)
                .toCompletableFuture()
                .get(5, TimeUnit.SECONDS));
  }

  @Test
  public void testSinkWithoutParallelismAndReadBackWithSource() throws Exception {
    final Sink<User, CompletionStage<Done>> slickSink = Slick.sink(session, insertUser);
    usersSource.runWith(slickSink, materializer).toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertEqualsUsers();
  }

  @Test
  public void testSinkPSWithoutParallelismAndReadBackWithSource() throws Exception {
    final Sink<User, CompletionStage<Done>> slickSink = Slick.sink(session, insertUserPS);
    usersSource.runWith(slickSink, materializer).toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertEqualsUsers();
  }

  @Test
  public void testSinkWithParallelismOf4AndReadBackWithSource() throws Exception {
    final Sink<User, CompletionStage<Done>> slickSink = Slick.sink(session, 4, insertUser);
    usersSource.runWith(slickSink, materializer).toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertEqualsUsers();
  }

  @Test
  public void testSinkPSWithParallelismOf4AndReadBackWithSource() throws Exception {
    final Sink<User, CompletionStage<Done>> slickSink = Slick.sink(session, 4, insertUserPS);
    usersSource.runWith(slickSink, materializer).toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertEqualsUsers();
  }

  @Test
  public void testFlowPSWithRecover() throws Exception {
    final Flow<User, Integer, NotUsed> slickFlow =
        Slick.<User>flow(
                session,
                (__, connection) ->
                    connection.prepareStatement(
                        "INSERT INTO ALPAKKA_SLICK_JAVADSL_TEST_USERS VALUES (?, ?)"))
            .recover(
                new PFBuilder<Throwable, Integer>().match(SQLException.class, (ex) -> -1).build());
    final List<Integer> insertionResult =
        usersSource
            .via(slickFlow)
            .runWith(Sink.seq(), materializer)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS);

    assertEquals(insertionResult.size(), 1);
    assertEquals(insertionResult.get(0), Integer.valueOf(-1));
  }

  @Test
  public void testFlowWithoutParallelismAndReadBackWithSource() throws Exception {
    final Flow<User, Integer, NotUsed> slickFlow = Slick.flow(session, insertUser);
    final List<Integer> insertionResult =
        usersSource
            .via(slickFlow)
            .runWith(Sink.seq(), materializer)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS);

    assertEquals(users.size(), insertionResult.size());
    assertEqualsUsers();
  }

  @Test
  public void testFlowPSWithoutParallelismAndReadBackWithSource() throws Exception {
    final Flow<User, Integer, NotUsed> slickFlow = Slick.flow(session, insertUserPS);
    final List<Integer> insertionResult =
        usersSource
            .via(slickFlow)
            .runWith(Sink.seq(), materializer)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS);

    assertEquals(users.size(), insertionResult.size());
    assertEqualsUsers();
  }

  @Test
  public void testFlowWithParallelismOf4AndReadBackWithSource() throws Exception {
    final Flow<User, Integer, NotUsed> slickFlow = Slick.flow(session, 4, insertUser);
    final List<Integer> insertionResult =
        usersSource
            .via(slickFlow)
            .runWith(Sink.seq(), materializer)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS);

    assertEquals(users.size(), insertionResult.size());
    assertEqualsUsers();
  }

  @Test
  public void testFlowPSWithParallelismOf4AndReadBackWithSource() throws Exception {
    final Flow<User, Integer, NotUsed> slickFlow = Slick.flow(session, 4, insertUserPS);
    final List<Integer> insertionResult =
        usersSource
            .via(slickFlow)
            .runWith(Sink.seq(), materializer)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS);

    assertEquals(users.size(), insertionResult.size());
    assertEqualsUsers();
  }

  @Test
  public void testFlowWithPassThroughWithoutParallelismAndReadBackWithSource() throws Exception {
    final Flow<User, User, NotUsed> slickFlow =
        Slick.flowWithPassThrough(session, system.dispatcher(), insertUser, (user, i) -> user);
    final List<User> insertedUsers =
        usersSource
            .via(slickFlow)
            .runWith(Sink.seq(), materializer)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS);

    assertEquals(SlickTest.users, new HashSet<>(insertedUsers));
    assertEqualsUsers();
  }

  @Test
  public void testFlowPSWithPassThroughWithoutParallelismAndReadBackWithSource() throws Exception {
    final Flow<User, User, NotUsed> slickFlow =
        Slick.flowWithPassThrough(session, system.dispatcher(), insertUserPS, (user, i) -> user);
    final List<User> insertedUsers =
        usersSource
            .via(slickFlow)
            .runWith(Sink.seq(), materializer)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS);

    assertEquals(SlickTest.users, new HashSet<>(insertedUsers));
    assertEqualsUsers();
  }

  @Test
  public void testFlowWithPassThroughWithParallelismOf4AndReadBackWithSource() throws Exception {
    final Flow<User, User, NotUsed> slickFlow =
        Slick.flowWithPassThrough(session, system.dispatcher(), 4, insertUser, (user, i) -> user);
    final List<User> insertedUsers =
        usersSource
            .via(slickFlow)
            .runWith(Sink.seq(), materializer)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS);

    assertEquals(users, new HashSet<>(insertedUsers));
    assertEqualsUsers();
  }

  @Test
  public void testFlowPSWithPassThroughWithParallelismOf4AndReadBackWithSource() throws Exception {
    final Flow<User, User, NotUsed> slickFlow =
        Slick.flowWithPassThrough(session, system.dispatcher(), 4, insertUserPS, (user, i) -> user);
    final List<User> insertedUsers =
        usersSource
            .via(slickFlow)
            .runWith(Sink.seq(), materializer)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS);

    assertEquals(users, new HashSet<>(insertedUsers));
    assertEqualsUsers();
  }

  @Test
  public void testFlowWithPassThroughKafkaExample() throws Exception {
    final List<User> usersList = new ArrayList<>(SlickTest.users);

    final List<KafkaMessage<User>> messagesFromKafka =
        usersList.stream()
            .map(user -> new KafkaMessage<>(user, new KafkaOffset(usersList.indexOf(user))))
            .collect(Collectors.toList());

    final Function<KafkaMessage<User>, String> insertUserInKafkaMessage =
        insertUser.compose((KafkaMessage<User> kafkaMessage) -> kafkaMessage.msg);

    List<KafkaOffset> committedOffsets = new ArrayList<>();

    final Function<KafkaOffset, CompletableFuture<Done>> commitToKafka =
        offset -> {
          committedOffsets.add(offset);
          return CompletableFuture.completedFuture(Done.getInstance());
        };

    final CompletionStage<Done> resultFuture =
        Source.from(messagesFromKafka)
            .via(
                Slick.flowWithPassThrough(
                    session,
                    system.dispatcher(),
                    insertUserInKafkaMessage,
                    (kafkaMessage, insertCount) -> kafkaMessage.map(user -> insertCount)))
            .mapAsync(
                1,
                kafkaMessage -> {
                  if (kafkaMessage.msg == 0) throw new Exception("Failed to write message to db");
                  return commitToKafka.apply(kafkaMessage.offset);
                })
            .runWith(Sink.ignore(), materializer);
    resultFuture.toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertEquals(users.size(), committedOffsets.size());
    assertEqualsUsers();
  }

  private void assertEqualsUsers() throws Exception {
    final Source<User, NotUsed> slickSource =
        Slick.source(
            session, selectAllUsers, (SlickRow row) -> new User(row.nextInt(), row.nextString()));

    final CompletionStage<List<User>> foundUsersFuture =
        slickSource.runWith(Sink.seq(), materializer);
    final Set<User> foundUsers =
        new HashSet<>(foundUsersFuture.toCompletableFuture().get(3, TimeUnit.SECONDS));

    assertEquals(foundUsers, users);
  }

  private static void executeStatement(
      String statement, SlickSession session, Materializer materializer) {
    try {
      Source.single(statement)
          .runWith(Slick.sink(session), materializer)
          .toCompletableFuture()
          .get(3, TimeUnit.SECONDS);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // For kafka example test case
  static class KafkaOffset {
    private Integer offset;

    public KafkaOffset(Integer offset) {
      this.offset = offset;
    }
  }

  static class KafkaMessage<A> {
    final A msg;
    final KafkaOffset offset;

    public KafkaMessage(A msg, KafkaOffset offset) {
      this.msg = msg;
      this.offset = offset;
    }

    public <B> KafkaMessage<B> map(Function<A, B> f) {
      return new KafkaMessage<>(f.apply(msg), offset);
    }
  }
}
