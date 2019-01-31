/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
// #atMostOnce #atLeastOnce
import akka.stream.alpakka.ironmq.*;
import akka.stream.alpakka.ironmq.javadsl.*;
// #atMostOnce #atLeastOnce
import akka.stream.alpakka.ironmq.impl.IronMqClientForJava;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;
import org.junit.AfterClass;
import org.junit.Test;
import scala.concurrent.Await;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class IronMqDocsTest extends IronMqClientForJava {
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
    Queue queue = Queue.ofName("alpakka-java");
    String name = queue.name();
    Await.result(givenQueue(name), awaiting);

    CompletionStage<Done> produced =
        Source.from(messages)
            .map(PushMessage::create)
            .runWith(IronMqProducer.producerSink(name, ironMqSettings), materializer);
    await(produced, patience);

    // #atMostOnce

    Source<Message, NotUsed> source = IronMqConsumer.atMostOnceConsumerSource(name, ironMqSettings);

    CompletionStage<List<Message>> receivedMessages =
        source.take(5).runWith(Sink.seq(), materializer);
    // #atMostOnce

    List<Message> result = await(receivedMessages, patience);
    assertEquals(result.size(), messages.size());
  }

  @Test
  public void atLeastOnce() throws Exception {
    Queue queue = Queue.ofName("alpakka-java-committing");
    String name = queue.name();
    Await.result(givenQueue(name), awaiting);

    CompletionStage<Done> produced =
        Source.from(messages)
            .map(PushMessage::create)
            .runWith(IronMqProducer.producerSink(name, ironMqSettings), materializer);
    await(produced, patience);

    // #atLeastOnce

    Source<CommittableMessage, NotUsed> source =
        IronMqConsumer.atLeastOnceConsumerSource(name, ironMqSettings);

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
    Queue queue = Await.result(givenQueue(), awaiting);

    // #flow
    CompletionStage<List<String>> producedIds =
        Source.from(messages)
            .map(PushMessage::create)
            .via(IronMqProducer.producerFlow(queue.name(), ironMqSettings))
            .runWith(Sink.seq(), materializer);

    // #flow
    assertEquals(messages.size(), await(producedIds, patience).size());

    CompletionStage<List<Message>> receivedMessages =
        IronMqConsumer.atMostOnceConsumerSource(queue.name(), ironMqSettings)
            .take(messages.size())
            .runWith(Sink.seq(), materializer);

    List<Message> result = await(receivedMessages, patience);
    assertEquals(messages.size(), result.size());
  }

  @Test
  public void atLeastOnceFlow() throws Exception {
    Queue sourceQueue = Await.result(givenQueue(), awaiting);
    Queue targetQueue = Await.result(givenQueue(), awaiting);

    CompletionStage<Done> produced =
        Source.from(messages)
            .map(PushMessage::create)
            .runWith(IronMqProducer.producerSink(sourceQueue.name(), ironMqSettings), materializer);
    assertEquals(Done.getInstance(), await(produced, patience));

    // #atLeastOnceFlow

    Flow<CommittablePushMessage<CommittableMessage>, String, NotUsed> pushAndCommit =
        IronMqProducer.<CommittableMessage>atLeastOnceProducerFlow(
            targetQueue.name(), ironMqSettings);

    CompletionStage<List<String>> producedIds =
        IronMqConsumer.atLeastOnceConsumerSource(sourceQueue.name(), ironMqSettings)
            .take(messages.size())
            .map(
                committableMessage ->
                    CommittablePushMessage.apply(
                        PushMessage.create(committableMessage.message().body()),
                        committableMessage))
            .via(pushAndCommit)
            .runWith(Sink.seq(), materializer);
    // #atLeastOnceFlow
    assertEquals(messages.size(), await(producedIds, patience).size());

    CompletionStage<List<Message>> receivedMessages =
        IronMqConsumer.atMostOnceConsumerSource(targetQueue.name(), ironMqSettings)
            .take(messages.size())
            .runWith(Sink.seq(), materializer);

    assertEquals(messages.size(), await(receivedMessages, patience).size());
  }

  @Test
  public void sink() throws Exception {
    Queue queue = Await.result(givenQueue(), awaiting);

    // #sink
    CompletionStage<Done> producedIds =
        Source.from(messages)
            .map(PushMessage::create)
            .runWith(IronMqProducer.producerSink(queue.name(), ironMqSettings), materializer);
    // #sink
    assertEquals(Done.getInstance(), await(producedIds, patience));

    CompletionStage<List<Message>> receivedMessages =
        IronMqConsumer.atMostOnceConsumerSource(queue.name(), ironMqSettings)
            .take(messages.size())
            .runWith(Sink.seq(), materializer);

    List<Message> result = await(receivedMessages, patience);
    assertEquals(messages.size(), result.size());
  }
}
