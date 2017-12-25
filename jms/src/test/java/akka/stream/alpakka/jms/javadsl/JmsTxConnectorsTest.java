/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.KillSwitch;
import akka.stream.Materializer;
import akka.stream.alpakka.jms.*;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.JavaTestKit;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class JmsTxConnectorsTest {

    //#create-test-message-list
    private List<JmsTextMessage> createTestMessageList() {
        List<Integer> intsIn = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        List<JmsTextMessage> msgsIn = new ArrayList<>();
        for(Integer n: intsIn) {
            Map<String, Object> properties = new HashMap<String, Object>();
            properties.put("Number", n);
            properties.put("IsOdd", n % 2 == 1);
            properties.put("IsEven", n % 2 == 0);

            msgsIn.add(JmsTextMessage.create(n.toString(), properties));
        }

        return msgsIn;
    }
    //#create-test-message-list

    @Test
    public void publishAndConsume() throws Exception {
        withServer(ctx -> {
            //#connection-factory
//            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(ctx.url);
            ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(ctx.url);
            //#connection-factory

            //#create-text-sink
            Sink<String, NotUsed> jmsSink = JmsSink.textSink(
                    JmsSinkSettings
                            .create(connectionFactory)
                            .withQueue("test")
            );
            //#create-text-sink

            //#run-text-sink
            List<String> in = Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k");
            Source.from(in).runWith(jmsSink, materializer);
            //#run-text-sink

            //#create-text-source
            Source<TxEnvelope, KillSwitch> jmsSource = JmsSource
                    .txSource(JmsSourceSettings
                            .create(connectionFactory)
                            .withSessionCount(5)
                            .withQueue("test")
                    );
            //#create-text-source

            //#run-text-source
            CompletionStage<List<String>> result = jmsSource
                    .take(in.size())
                    .map(env -> new Pair<>(env, ((TextMessage) env.message()).getText()))
                    .map(pair -> { pair.first().commit(); return pair.second(); })
                    .runWith(Sink.seq(), materializer);
            //#run-text-source
            List<String> out = new ArrayList<>(result.toCompletableFuture().get(3, TimeUnit.SECONDS));
            Collections.sort(out);
            assertEquals(in, out);
        });

    }

    @Test
    public void publishAndConsumeJmsTextMessagesWithProperties() throws Exception {
        withServer(ctx -> {
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(ctx.url);

            //#create-jms-sink
            Sink<JmsTextMessage, NotUsed> jmsSink = JmsSink.create(
                    JmsSinkSettings
                            .create(connectionFactory)
                            .withQueue("test")
            );
            //#create-jms-sink

            //#create-messages-with-properties
            List<JmsTextMessage> msgsIn = createTestMessageList();
            //#create-messages-with-properties

            //#run-jms-sink
            Source.from(msgsIn).runWith(jmsSink, materializer);
            //#run-jms-sink

            //#create-jms-source
            Source<TxEnvelope, KillSwitch> jmsSource = JmsSource.txSource(JmsSourceSettings
                    .create(connectionFactory)
                    .withSessionCount(5)
                    .withQueue("test")
            );
            //#create-jms-source

            //#run-jms-source
            CompletionStage<List<Message>> result = jmsSource
                    .take(msgsIn.size())
                    .map(env -> { env.commit(); return env.message(); })
                    .runWith(Sink.seq(), materializer);
            //#run-jms-source

            List<Message> outMessages = new ArrayList<>(result.toCompletableFuture().get(3, TimeUnit.SECONDS));
            outMessages.sort((a, b) -> {
                try {
                    return a.getIntProperty("Number") - b.getIntProperty("Number");
                } catch (JMSException e) {
                    throw new RuntimeException(e);
                }
            });

            int msgIdx = 0;
            for(Message outMsg: outMessages) {
                assertEquals(outMsg.getIntProperty("Number"), msgsIn.get(msgIdx).properties().get("Number").get());
                assertEquals(outMsg.getBooleanProperty("IsOdd"), msgsIn.get(msgIdx).properties().get("IsOdd").get());
                assertEquals(outMsg.getBooleanProperty("IsEven"), (msgsIn.get(msgIdx).properties().get("IsEven").get()));
                msgIdx++;
            }
        });
    }

    @Test
    public void publishAndConsumeJmsTextMessagesWithHeaders() throws Exception {
        withServer(ctx -> {
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(ctx.url);

            //#create-jms-sink
            Sink<JmsTextMessage, NotUsed> jmsSink = JmsSink.create(
                    JmsSinkSettings
                            .create(connectionFactory)
                            .withQueue("test")
            );
            //#create-jms-sink

            //#create-messages-with-properties
            List<JmsTextMessage> msgsIn = createTestMessageList().stream()
                    .map(jmsTextMessage -> jmsTextMessage.withHeader(JmsType.create("type")))
                    .map(jmsTextMessage -> jmsTextMessage.withHeader(JmsCorrelationId.create("correlationId")))
                    .map(jmsTextMessage -> jmsTextMessage.withHeader(JmsReplyTo.queue("test-reply")))
                    .collect(Collectors.toList());
            //#create-messages-with-properties

            //#run-jms-sink
            Source.from(msgsIn).runWith(jmsSink, materializer);
            //#run-jms-sink

            //#create-jms-source
            Source<TxEnvelope, KillSwitch> jmsSource = JmsSource.txSource(JmsSourceSettings
                    .create(connectionFactory)
                    .withSessionCount(5)
                    .withQueue("test")
            );
            //#create-jms-source

            //#run-jms-source
            CompletionStage<List<Message>> result = jmsSource
                    .take(msgsIn.size())
                    .map(env -> { env.commit(); return env.message(); })
                    .runWith(Sink.seq(), materializer);
            //#run-jms-source

            List<Message> outMessages = new ArrayList<>(result.toCompletableFuture().get(3, TimeUnit.SECONDS));
            outMessages.sort((a, b) -> {
                try {
                    return a.getIntProperty("Number") - b.getIntProperty("Number");
                } catch (JMSException e) {
                    throw new RuntimeException(e);
                }
            });
            int msgIdx = 0;
            for(Message outMsg: outMessages) {
                assertEquals(outMsg.getIntProperty("Number"), msgsIn.get(msgIdx).properties().get("Number").get());
                assertEquals(outMsg.getBooleanProperty("IsOdd"), msgsIn.get(msgIdx).properties().get("IsOdd").get());
                assertEquals(outMsg.getBooleanProperty("IsEven"), (msgsIn.get(msgIdx).properties().get("IsEven").get()));
                assertEquals(outMsg.getJMSType(), "type");
                assertEquals(outMsg.getJMSCorrelationID(), "correlationId");
                assertEquals(((ActiveMQQueue) outMsg.getJMSReplyTo()).getQueueName(), "test-reply");
                msgIdx++;
            }
        });
    }

    @Test
    public void publishJmsTextMessagesWithPropertiesAndConsumeThemWithASelector() throws Exception {
        withServer(ctx -> {
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(ctx.url);

            Sink<JmsTextMessage, NotUsed> jmsSink = JmsSink.create(
                    JmsSinkSettings
                            .create(connectionFactory)
                            .withQueue("test")
            );

            List<JmsTextMessage> msgsIn = createTestMessageList();

            Source.from(msgsIn).runWith(jmsSink, materializer);

            //#create-jms-source-with-selector
            Source<TxEnvelope, KillSwitch> jmsSource = JmsSource.txSource(JmsSourceSettings
                    .create(connectionFactory)
                    .withSessionCount(5)
                    .withQueue("test")
                    .withSelector("IsOdd = TRUE")
            );
            //#create-jms-source-with-selector

            //#assert-only-odd-messages-received
            List<JmsTextMessage> oddMsgsIn = msgsIn.stream()
                    .filter(msg -> Integer.valueOf(msg.body()) % 2 == 1)
                    .collect(Collectors.toList());
            assertEquals(5, oddMsgsIn.size());

            CompletionStage<List<Message>> result = jmsSource
                    .take(oddMsgsIn.size())
                    .map(env -> { env.commit(); return env.message(); })
                    .runWith(Sink.seq(), materializer);

            List<Message> outMessages = new ArrayList<>(result.toCompletableFuture().get(3, TimeUnit.SECONDS));
            outMessages.sort((a, b) -> {
                try {
                    return a.getIntProperty("Number") - b.getIntProperty("Number");
                } catch (JMSException e) {
                    throw new RuntimeException(e);
                }
            });

            int msgIdx = 0;
            for(Message outMsg: outMessages) {
                assertEquals(outMsg.getIntProperty("Number"), oddMsgsIn.get(msgIdx).properties().get("Number").get());
                assertEquals(outMsg.getBooleanProperty("IsOdd"), oddMsgsIn.get(msgIdx).properties().get("IsOdd").get());
                assertEquals(outMsg.getBooleanProperty("IsEven"), (oddMsgsIn.get(msgIdx).properties().get("IsEven").get()));
                assertEquals(1, outMsg.getIntProperty("Number") % 2);
                msgIdx++;
            }
            //#assert-only-odd-messages-received
        });
    }

    @Test
    public void publishAndConsumeTopic() throws Exception {
        withServer(ctx -> {
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(ctx.url);

            List<String> in = Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k");
            List<String> inNumbers = IntStream.range(0, 10).boxed().map(String::valueOf).collect(Collectors.toList());

            //#create-topic-sink
            Sink<String, NotUsed> jmsTopicSink = JmsSink.textSink(
                    JmsSinkSettings
                            .create(connectionFactory)
                            .withTopic("topic")
            );
            //#create-topic-sink
            Sink<String, NotUsed> jmsTopicSink2 = JmsSink.textSink(
                    JmsSinkSettings
                            .create(connectionFactory)
                            .withTopic("topic")
            );

            //#create-topic-source
            Source<TxEnvelope, KillSwitch> jmsTopicSource = JmsSource
                    .txSource(JmsSourceSettings
                            .create(connectionFactory)
                            .withSessionCount(1)
                            .withTopic("topic")
                    );
            //#create-topic-source
            Source<TxEnvelope, KillSwitch> jmsTopicSource2 = JmsSource
                    .txSource(JmsSourceSettings
                            .create(connectionFactory)
                            .withSessionCount(1)
                            .withTopic("topic")
                    );

            //#run-topic-source
            CompletionStage<List<String>> result = jmsTopicSource
                    .take(in.size() + inNumbers.size())
                    .map(env -> {
                        env.commit();
                        return ((TextMessage) env.message()).getText();
                    })
                    .runWith(Sink.seq(), materializer)
                    .thenApply(l -> l.stream().sorted().collect(Collectors.toList()));
            //#run-topic-source
            CompletionStage<List<String>> result2 = jmsTopicSource2
                    .take(in.size() + inNumbers.size())
                    .map(env -> {
                        env.commit();
                        return ((TextMessage) env.message()).getText();
                    })
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
            Thread.sleep(500);
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
