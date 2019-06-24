/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.slick.javadsl.Slick;
import akka.stream.alpakka.slick.javadsl.SlickSession;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

// We're going to pretend we got messages from kafka.
// After we've written them to a db with Slick, we want
// to commit the offset to Kafka
public class DocSnippetFlowWithPassThrough {

  // mimics a Kafka 'Committable' type
  static class CommittableOffset {
    private Integer offset;

    public CommittableOffset(Integer offset) {
      this.offset = offset;
    }

    public CompletableFuture<Done> commit() {
      return CompletableFuture.completedFuture(Done.getInstance());
    }
  }

  static class KafkaMessage<A> {
    final A msg;
    final CommittableOffset offset;

    public KafkaMessage(A msg, CommittableOffset offset) {
      this.msg = msg;
      this.offset = offset;
    }

    public <B> KafkaMessage<B> map(Function<A, B> f) {
      return new KafkaMessage(f.apply(msg), offset);
    }
  }

  public static void main(String[] args) throws Exception {
    final ActorSystem system = ActorSystem.create();
    final Materializer materializer = ActorMaterializer.create(system);

    final SlickSession session = SlickSession.forConfig("slick-h2");
    system.registerOnTermination(session::close);

    final List<User> users =
        IntStream.range(0, 42)
            .boxed()
            .map((i) -> new User(i, "Name" + i))
            .collect(Collectors.toList());

    List<KafkaMessage<User>> messagesFromKafka =
        users.stream()
            .map(user -> new KafkaMessage<>(user, new CommittableOffset(users.indexOf(user))))
            .collect(Collectors.toList());

    // #flowWithPassThrough-example
    final CompletionStage<Done> done =
        Source.from(messagesFromKafka)
            .via(
                Slick.flowWithPassThrough(
                    session,
                    system.dispatcher(),
                    // add an optional second argument to specify the parallelism factor (int)
                    (kafkaMessage) ->
                        "INSERT INTO ALPAKKA_SLICK_JAVADSL_TEST_USERS VALUES ("
                            + kafkaMessage.msg.id
                            + ", '"
                            + kafkaMessage.msg.name
                            + "')",
                    (kafkaMessage, insertCount) ->
                        kafkaMessage.map(
                            user ->
                                Pair.create(
                                    user,
                                    insertCount)) // allows to keep the kafka message offset so it
                    // can be committed in a next stage
                    ))
            .log("nr-of-updated-rows")
            .mapAsync(
                1,
                kafkaMessage ->
                    kafkaMessage.offset.commit()) // in correct order, commit Kafka message
            .runWith(Sink.ignore(), materializer);
    // #flowWithPassThrough-example

    done.whenComplete(
        (value, exception) -> {
          system.terminate();
        });
  }
}
