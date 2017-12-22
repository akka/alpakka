/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.ironmq.javadsl;

import akka.stream.alpakka.ironmq.IronMqSettings;
import akka.stream.alpakka.ironmq.PushMessage;
import akka.stream.alpakka.ironmq.Queue;
import akka.stream.alpakka.ironmq.UnitTest;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
public class IronMqConsumerTest extends UnitTest {

    @Test
    public void ironMqConsumerShouldBeNiceToMe() throws Exception {

        Queue from = givenQueue();
        Queue to = givenQueue();
        givenMessages(from.name(), 100);

        IronMqSettings settings = IronMqSettings.create(getActorSystem());

        int expectedNumberOfMessages = 10;

        int numberOfMessages =  IronMqConsumer.atLeastOnceConsumerSource(from.name(), settings)
                .take(expectedNumberOfMessages)
                .map(cm -> new CommittablePushMessage<>(PushMessage.create(cm.message().body()), cm))
                .alsoToMat(Sink.fold(0, (x,y) -> x + 1), Keep.right())
                .to(IronMqProducer.atLeastOnceProducerSink(to.name(), settings))
                .run(getMaterializer())
                .toCompletableFuture().get(10, TimeUnit.SECONDS);

        assertThat(numberOfMessages, is(expectedNumberOfMessages));

    }
}
