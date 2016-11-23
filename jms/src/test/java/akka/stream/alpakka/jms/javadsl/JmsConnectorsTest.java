/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
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
import scala.concurrent.duration.FiniteDuration;

import javax.jms.JMSException;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static akka.pattern.PatternsCS.after;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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
    public void applyingBackpressure() throws Exception {
        withServer(ctx -> {
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(ctx.url);
            List<String> in = Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k");
            Source.from(in).runWith(JmsSink.create(JmsSinkSettings
                    .create(connectionFactory)
                    .withQueue("test")
            ), materializer);

            CompletionStage<List<String>> result = JmsSource
                    .textSource(JmsSourceSettings
                            .create(connectionFactory)
                            .withQueue("test")
                            .withBufferSize(1)
                    )
                    .mapAsync(1, e ->
                            after(FiniteDuration.create(1, TimeUnit.SECONDS), system.scheduler(), system.dispatcher(), CompletableFuture.completedFuture(e))
                    )
                    .take(in.size()).runWith(Sink.seq(), materializer);

            assertEquals(in, result.toCompletableFuture().get(15, TimeUnit.SECONDS));
        });
    }

    @Test
    public void deconnexionShouldFail() throws Exception {
        withServer(ctx -> {
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(ctx.url);

            CompletionStage<List<String>> result = JmsSource.textSource(JmsSourceSettings
                    .create(connectionFactory)
                    .withQueue("test")
                    .withBufferSize(1)
            ).runWith(Sink.seq(), materializer);
            Thread.sleep(500);
            ctx.broker.stop();
            try {
                result.toCompletableFuture().get();
                fail();
            } catch (ExecutionException e) {
                assertEquals(JMSException.class, e.getCause().getClass());
            }
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

            //#run-topic-sink
            Source.from(in).runWith(jmsTopicSink, materializer);
            //#run-topic-sink
            Source.from(inNumbers).runWith(jmsTopicSink2, materializer);


            assertEquals(Stream.concat(in.stream(), inNumbers.stream()).sorted().collect(Collectors.toList()), result.toCompletableFuture().get(3, TimeUnit.SECONDS));
            assertEquals(Stream.concat(in.stream(), inNumbers.stream()).sorted().collect(Collectors.toList()), result2.toCompletableFuture().get(3, TimeUnit.SECONDS));
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
        Integer port = getPort();
        broker.setBrokerName(host);
        broker.setUseJmx(false);
        String url = "tcp://" + host + ":" + port;
        broker.addConnector(url);
        broker.start();
        try {
            test.accept(new Context(host, port, url, broker));
        } finally {
            if(broker.isStarted()) {
                broker.stop();
            }
        }
    }

    private Integer getPort() throws IOException {
        ServerSocket serverSocket = new ServerSocket(0);
        return serverSocket.getLocalPort();
    }

    @FunctionalInterface
    private interface ConsumerChecked<T> {
        void accept(T elt) throws Exception;
    }

    private static class Context {
        final String host;
        final String url;
        final Integer port;
        final BrokerService broker;

        public Context(String host, Integer port, String url, BrokerService broker) {
            this.host = host;
            this.port = port;
            this.url = url;
            this.broker = broker;
        }
    }
}
