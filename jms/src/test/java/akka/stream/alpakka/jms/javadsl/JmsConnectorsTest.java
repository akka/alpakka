/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.jms.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.jms.JmsSinkSettings;
import akka.stream.alpakka.jms.JmsSourceSettings;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.JavaTestKit;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class JmsConnectorsTest {

    @Test
    public void publishAndConsume() throws Exception {
        withServer(ctx -> {
            //#connection-factory
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(ctx.url);
            //#connection-factory

            //#create-sink
            Sink<String, NotUsed> jmsSink = JmsSink.create(
                    JmsSinkSettings
                            .create(connectionFactory)
                            .withQueue("test")
            );
            //#create-sink

            //#run-sink
            List<String> in = Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k");
            Source.from(in).runWith(jmsSink, materializer);
            //#run-sink

            //#create-source
            Source<String, NotUsed> jmsSource = JmsSource
                    .textSource(JmsSourceSettings
                            .create(connectionFactory)
                            .withQueue("test")
                            .withBufferSize(10)
                    );
            //#create-source

            //#run-source
            CompletionStage<List<String>> result = jmsSource
                    .take(in.size())
                    .runWith(Sink.seq(), materializer);
            //#run-source

            assertEquals(in, result.toCompletableFuture().get(3, TimeUnit.SECONDS));
        });

    }

    @Test
    public void publishAndConsumeTopic() throws Exception {
        withServer(ctx -> {
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(ctx.url);

            List<String> in = Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k");
            List<String> inNumbers = IntStream.range(0, 10).boxed().map(String::valueOf).collect(Collectors.toList());

            //#create-topic-sink
            Sink<String, NotUsed> jmsTopicSink = JmsSink.create(
                    JmsSinkSettings
                            .create(connectionFactory)
                            .withTopic("topic")
            );
            //#create-topic-sink
            Sink<String, NotUsed> jmsTopicSink2 = JmsSink.create(
                    JmsSinkSettings
                            .create(connectionFactory)
                            .withTopic("topic")
            );

            //#create-topic-source
            Source<String, NotUsed> jmsTopicSource = JmsSource
                    .textSource(JmsSourceSettings
                            .create(connectionFactory)
                            .withTopic("topic")
                            .withBufferSize(10)
                    );
            //#create-topic-source
            Source<String, NotUsed> jmsTopicSource2 = JmsSource
                    .textSource(JmsSourceSettings
                            .create(connectionFactory)
                            .withTopic("topic")
                            .withBufferSize(10)
                    );

            //#run-topic-source
            CompletionStage<List<String>> result = jmsTopicSource
                    .take(in.size() + inNumbers.size())
                    .runWith(Sink.seq(), materializer)
                    .thenApply(l -> l.stream().sorted().collect(Collectors.toList()));
            //#run-topic-source
            CompletionStage<List<String>> result2 = jmsTopicSource2
                    .take(in.size() + inNumbers.size())
                    .runWith(Sink.seq(), materializer)
                    .thenApply(l -> l.stream().sorted().collect(Collectors.toList()));

            Thread.sleep(500);

            //#run-topic-sink
            Source.from(in).runWith(jmsTopicSink, materializer);
            //#run-topic-sink
            Source.from(inNumbers).runWith(jmsTopicSink2, materializer);


            assertEquals(Stream.concat(in.stream(), inNumbers.stream()).sorted().collect(Collectors.toList()), result.toCompletableFuture().get(5, TimeUnit.SECONDS));
            assertEquals(Stream.concat(in.stream(), inNumbers.stream()).sorted().collect(Collectors.toList()), result2.toCompletableFuture().get(5, TimeUnit.SECONDS));
        });
    }


    private static ActorSystem system;
    private static Materializer materializer;

    @BeforeClass
    public static void setup() throws Exception {
        system = ActorSystem.create();
        materializer = ActorMaterializer.create(system);
    }

    @AfterClass
    public static void teardown() throws Exception {
        JavaTestKit.shutdownActorSystem(system);
    }

    private void withServer(ConsumerChecked<Context> test) throws Exception {
        BrokerService broker = new BrokerService();
        broker.setPersistent(false);
        String host = "localhost";
        Integer port = akka.testkit.SocketUtil.temporaryServerAddress(host, false).getPort();
        broker.setBrokerName(host);
        broker.setUseJmx(false);
        String url = "tcp://" + host + ":" + port;
        broker.addConnector(url);
        broker.start();
        try {
            test.accept(new Context(url, broker));
        } finally {
            if(broker.isStarted()) {
                broker.stop();
            }
        }
    }

    @FunctionalInterface
    private interface ConsumerChecked<T> {
        void accept(T elt) throws Exception;
    }

    private static class Context {
        final String url;
        final BrokerService broker;

        public Context(String url, BrokerService broker) {
            this.url = url;
            this.broker = broker;
        }
    }
}
