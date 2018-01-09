/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.slick.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

/**
 * This unit test is run using a local H2 database using
 * `/tmp/alpakka-slick-h2-test` for temporary storage.
 */
public class SlickTest {
  private static ActorSystem system;
  private static Materializer materializer;

  //#init-session
  private static final SlickSession session = SlickSession.forConfig("slick-h2");
  //#init-session

  private static final Set<User> users = IntStream.range(0, 40).boxed().map((i) -> new User(i, "Name"+i)).collect(Collectors.toSet());
  private static final Source<User, NotUsed> usersSource = Source.from(users);

  private static final Function<User, String> insertUser = (user) -> "INSERT INTO ALPAKKA_SLICK_JAVADSL_TEST_USERS VALUES (" + user.id + ", '" + user.name + "')";

  private static final String selectAllUsers = "SELECT ID, NAME FROM ALPAKKA_SLICK_JAVADSL_TEST_USERS";


  @BeforeClass
  public static void setup() {
    //#init-mat
    system = ActorSystem.create();
    materializer = ActorMaterializer.create(system);
    //#init-mat

    executeStatement("CREATE TABLE ALPAKKA_SLICK_JAVADSL_TEST_USERS(ID INTEGER, NAME VARCHAR(50))", session, materializer);
  }

  @After
  public void cleanUp() {
    executeStatement("DELETE FROM ALPAKKA_SLICK_JAVADSL_TEST_USERS", session, materializer);
  }

  @AfterClass
  public static void teardown() {
    executeStatement("DROP TABLE ALPAKKA_SLICK_JAVADSL_TEST_USERS", session, materializer);

    //#close-session
    system.registerOnTermination(session::close);
    //#close-session

    TestKit.shutdownActorSystem(system);
  }

  @Test
  public void testSinkWithoutParallelismAndReadBackWithSource() throws Exception {
    final Sink<User, CompletionStage<Done>> slickSink = Slick.sink(session, insertUser);
    final CompletionStage<Done> insertionResultFuture = usersSource.runWith(slickSink, materializer);

    insertionResultFuture.toCompletableFuture().get(5, TimeUnit.SECONDS);

    final Source<User, NotUsed> slickSource = Slick.source(
      session,
      selectAllUsers,
      (SlickRow row) -> new User(row.nextInt(), row.nextString())
    );

    final CompletionStage<List<User>> foundUsersFuture = slickSource.runWith(Sink.seq(), materializer);
    final Set<User> foundUsers = new HashSet<>(foundUsersFuture.toCompletableFuture().get(3, TimeUnit.SECONDS));

    assertEquals(foundUsers, users);
  }

  @Test
  public void testSinkWithParallelismOf4AndReadBackWithSource() throws Exception {
    final Sink<User, CompletionStage<Done>> slickSink = Slick.sink(session, 4, insertUser);
    final CompletionStage<Done> insertionResultFuture = usersSource.runWith(slickSink, materializer);

    insertionResultFuture.toCompletableFuture().get(5, TimeUnit.SECONDS);

    final Source<User, NotUsed> slickSource = Slick.source(
      session,
      selectAllUsers,
      (SlickRow row) -> new User(row.nextInt(), row.nextString())
    );

    final CompletionStage<List<User>> foundUsersFuture = slickSource.runWith(Sink.seq(), materializer);
    final Set<User> foundUsers = new HashSet<>(foundUsersFuture.toCompletableFuture().get(3, TimeUnit.SECONDS));

    assertEquals(foundUsers, users);
  }

  @Test
  public void testFlowWithoutParallelismAndReadBackWithSource() throws Exception {
    final Flow<User, Integer, NotUsed> slickFlow = Slick.flow(session, insertUser);
    final CompletionStage<List<Integer>> insertionResultFuture = usersSource.via(slickFlow).runWith(Sink.seq(), materializer);
    final List<Integer> insertionResult = insertionResultFuture.toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertEquals(users.size(), insertionResult.size());

    final Source<User, NotUsed> slickSource = Slick.source(
      session,
      selectAllUsers,
      (SlickRow row) -> new User(row.nextInt(), row.nextString())
    );

    final CompletionStage<List<User>> foundUsersFuture = slickSource.runWith(Sink.seq(), materializer);
    final Set<User> foundUsers = new HashSet<>(foundUsersFuture.toCompletableFuture().get(3, TimeUnit.SECONDS));

    assertEquals(foundUsers, users);
  }

  @Test
  public void testFlowWithParallelismOf4AndReadBackWithSource() throws Exception {
    final Flow<User, Integer, NotUsed> slickFlow = Slick.flow(session, 4, insertUser);
    final CompletionStage<List<Integer>> insertionResultFuture = usersSource.via(slickFlow).runWith(Sink.seq(), materializer);
    final List<Integer> insertionResult = insertionResultFuture.toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertEquals(users.size(), insertionResult.size());

    final Source<User, NotUsed> slickSource = Slick.source(
      session,
      selectAllUsers,
      (SlickRow row) -> new User(row.nextInt(), row.nextString())
    );

    final CompletionStage<List<User>> foundUsersFuture = slickSource.runWith(Sink.seq(), materializer);
    final Set<User> foundUsers = new HashSet<>(foundUsersFuture.toCompletableFuture().get(3, TimeUnit.SECONDS));

    assertEquals(foundUsers, users);
  }

  private static void executeStatement(String statement, SlickSession session, Materializer materializer) {
    try {
      Source
        .single(statement)
        .runWith(Slick.sink(session), materializer)
        .toCompletableFuture()
        .get(3, TimeUnit.SECONDS);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
  }
}
