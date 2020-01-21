/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
// #imports
import akka.stream.alpakka.ironmq.*;
import akka.stream.alpakka.ironmq.javadsl.*;

// #imports
import akka.stream.alpakka.ironmq.impl.IronMqClientForJava;
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import scala.concurrent.Await;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class IronMqDocsTest extends IronMqClientForJava {
  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  private static final ActorSystem system = ActorSystem.create();
  private static final Materializer materializer = ActorMaterializer.create(system);
  private static final scala.concurrent.duration.Duration awaiting =
      scala.concurrent.duration.Duration.create(5, TimeUnit.SECONDS);
  private static final Duration patience = Duration.ofSeconds(5);

  private static final List<String> messages =
      Arrays.asList("test-1", "test-2", "test-3", "test-4", "test-5");

  private final IronMqSettings ironMqSettings = ironMqSettings();

  public IronMqDocsTest() {
    super(system, materializer);
  }

  @AfterClass
  public static void afterAll() {
    TestKit.shutdownActorSystem(system);
  }

  private <T> T await(CompletionStage<T> cs, Duration d) throws Exception {
    return cs.toCompletableFuture().get(d.toMillis(), TimeUnit.MILLISECONDS);
  }

  @Test
  public void atMostOnce() throws Exception {
    String queueName = "alpakka-java";
    Await.result(givenQueue(queueName), awaiting);

    CompletionStage<Done> produced =
        Source.from(messages)
            .map(PushMessage::create)
            .runWith(IronMqProducer.sink(queueName, ironMqSettings), materializer);
    await(produced, patience);

    // #atMostOnce
    Source<Message, NotUsed> source = IronMqConsumer.atMostOnceSource(queueName, ironMqSettings);

    CompletionStage<List<Message>> receivedMessages =
        source.take(5).runWith(Sink.seq(), materializer);
    // #atMostOnce

    List<Message> result = await(receivedMessages, patience);
    assertEquals(result.size(), messages.size());
  }

  @Test
  public void atLeastOnce() throws Exception {
    String queueName = "alpakka-java-committing";
    Await.result(givenQueue(queueName), awaiting);

    CompletionStage<Done> produced =
        Source.from(messages)
            .map(PushMessage::create)
            .runWith(IronMqProducer.sink(queueName, ironMqSettings), materializer);
    await(produced, patience);

    // #atLeastOnce
    Source<CommittableMessage, NotUsed> source =
        IronMqConsumer.atLeastOnceSource(queueName, ironMqSettings);

    Flow<CommittableMessage, CommittableMessage, NotUsed> businessLogic =
        Flow.of(CommittableMessage.class); // do something useful with the received messages

    CompletionStage<List<Message>> receivedMessages =
        source
            .take(5)
            .via(businessLogic)
            .mapAsync(1, m -> m.commit().thenApply(d -> m.message()))
            .runWith(Sink.seq(), materializer);
    // #atLeastOnce

    List<Message> result = await(receivedMessages, patience);
    assertEquals(result.size(), messages.size());
  }

  @Test
  public void pushMessagesFlow() throws Exception {
    String queueName = Await.result(givenQueue(), awaiting);

    // #flow
    CompletionStage<List<String>> producedIds =
        Source.from(messages)
            .map(PushMessage::create)
            .via(IronMqProducer.flow(queueName, ironMqSettings))
            .runWith(Sink.seq(), materializer);

    // #flow
    assertEquals(messages.size(), await(producedIds, patience).size());

    CompletionStage<List<Message>> receivedMessages =
        IronMqConsumer.atMostOnceSource(queueName, ironMqSettings)
            .take(messages.size())
            .runWith(Sink.seq(), materializer);

    List<Message> result = await(receivedMessages, patience);
    assertEquals(messages.size(), result.size());
  }

  @Test
  public void atLeastOnceFlow() throws Exception {
    String sourceQueue = Await.result(givenQueue(), awaiting);
    String targetQueue = Await.result(givenQueue(), awaiting);

    CompletionStage<Done> produced =
        Source.from(messages)
            .map(PushMessage::create)
            .runWith(IronMqProducer.sink(sourceQueue, ironMqSettings), materializer);
    assertEquals(Done.getInstance(), await(produced, patience));

    // #atLeastOnceFlow
    Flow<CommittablePushMessage<CommittableMessage>, String, NotUsed> pushAndCommit =
        IronMqProducer.atLeastOnceFlow(targetQueue, ironMqSettings);

    CompletionStage<List<String>> producedIds =
        IronMqConsumer.atLeastOnceSource(sourceQueue, ironMqSettings)
            .take(messages.size())
            .map(CommittablePushMessage::create)
            .via(pushAndCommit)
            .runWith(Sink.seq(), materializer);
    // #atLeastOnceFlow
    assertEquals(messages.size(), await(producedIds, patience).size());

    CompletionStage<List<Message>> receivedMessages =
        IronMqConsumer.atMostOnceSource(targetQueue, ironMqSettings)
            .take(messages.size())
            .runWith(Sink.seq(), materializer);

    assertEquals(messages.size(), await(receivedMessages, patience).size());
  }

  @Test
  public void sink() throws Exception {
    String queueName = Await.result(givenQueue(), awaiting);

    // #sink
    CompletionStage<Done> producedIds =
        Source.from(messages)
            .map(PushMessage::create)
            .runWith(IronMqProducer.sink(queueName, ironMqSettings), materializer);
    // #sink
    assertEquals(Done.getInstance(), await(producedIds, patience));

    CompletionStage<List<Message>> receivedMessages =
        IronMqConsumer.atMostOnceSource(queueName, ironMqSettings)
            .take(messages.size())
            .runWith(Sink.seq(), materializer);

    List<Message> result = await(receivedMessages, patience);
    assertEquals(messages.size(), result.size());
  }
}
