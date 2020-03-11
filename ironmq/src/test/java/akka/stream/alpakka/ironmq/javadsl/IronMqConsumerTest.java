/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.ironmq.javadsl;

import akka.stream.alpakka.ironmq.IronMqSettings;
import akka.stream.alpakka.ironmq.PushMessage;
import akka.stream.alpakka.ironmq.UnitTest;
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class IronMqConsumerTest extends UnitTest {
  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  @Test
  public void ironMqConsumerShouldBeNiceToMe() throws Exception {

    String from = givenQueue();
    String to = givenQueue();
    givenMessages(from, 100);

    IronMqSettings settings = IronMqSettings.create(getActorSystem());

    int expectedNumberOfMessages = 10;

    int numberOfMessages =
        IronMqConsumer.atLeastOnceSource(from, settings)
            .take(expectedNumberOfMessages)
            .map(cm -> new CommittablePushMessage<>(PushMessage.create(cm.message().body()), cm))
            .alsoToMat(Sink.fold(0, (x, y) -> x + 1), Keep.right())
            .to(IronMqProducer.atLeastOnceSink(to, settings))
            .run(getMaterializer())
            .toCompletableFuture()
            .get(10, TimeUnit.SECONDS);

    assertThat(numberOfMessages, is(expectedNumberOfMessages));
  }
}
