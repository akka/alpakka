/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms.javadsl;

import akka.Done;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.jms.*;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;
import com.typesafe.config.Config;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import jmstestkit.JmsBroker;

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

public class JmsAckConnectorsTest {

  private static ActorSystem system;
  private static Materializer materializer;
  private static Config consumerConfig;
  private static Config producerConfig;

  @BeforeClass
  public static void setup() {
    system = ActorSystem.create();
    materializer = ActorMaterializer.create(system);
    consumerConfig = system.settings().config().getConfig(JmsConsumerSettings.configPath());
    producerConfig = system.settings().config().getConfig(JmsProducerSettings.configPath());
  }

  @AfterClass
  public static void teardown() {
    TestKit.shutdownActorSystem(system);
  }

  private List<JmsTextMessage> createTestMessageList() {
    List<Integer> intsIn = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    List<JmsTextMessage> msgsIn = new ArrayList<>();
    for (Integer n : intsIn) {
      msgsIn.add(
          JmsTextMessage.create(n.toString())
              .withProperty("Number", n)
              .withProperty("IsOdd", n % 2 == 1)
              .withProperty("IsEven", n % 2 == 0));
    }

    return msgsIn;
  }

  @Test
  public void publishAndConsume() throws Exception {
    withServer(
        server -> {
          ConnectionFactory connectionFactory = server.createConnectionFactory();

          Sink<String, CompletionStage<Done>> jmsSink =
              JmsProducer.textSink(
                  JmsProducerSettings.create(producerConfig, connectionFactory).withQueue("test"));

          List<String> in = Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k");
          Source.from(in).runWith(jmsSink, materializer);

          Source<AckEnvelope, JmsConsumerControl> jmsSource =
              JmsConsumer.ackSource(
                  JmsConsumerSettings.create(consumerConfig, connectionFactory)
                      .withSessionCount(5)
                      .withBufferSize(0)
                      .withQueue("test"));

          CompletionStage<List<String>> result =
              jmsSource
                  .take(in.size())
                  .map(env -> new Pair<>(env, ((TextMessage) env.message()).getText()))
                  .map(
                      pair -> {
                        pair.first().acknowledge();
                        return pair.second();
                      })
                  .runWith(Sink.seq(), materializer);
          List<String> out = new ArrayList<>(result.toCompletableFuture().get(3, TimeUnit.SECONDS));
          Collections.sort(out);
          assertEquals(in, out);
        });
  }

  @Test
  public void publishAndConsumeJmsTextMessagesWithProperties() throws Exception {
    withServer(
        server -> {
          ConnectionFactory connectionFactory = server.createConnectionFactory();

          Sink<JmsTextMessage, CompletionStage<Done>> jmsSink =
              JmsProducer.sink(
                  JmsProducerSettings.create(producerConfig, connectionFactory).withQueue("test"));

          List<JmsTextMessage> msgsIn = createTestMessageList();

          Source.from(msgsIn).runWith(jmsSink, materializer);

          Source<AckEnvelope, JmsConsumerControl> jmsSource =
              JmsConsumer.ackSource(
                  JmsConsumerSettings.create(consumerConfig, connectionFactory)
                      .withSessionCount(5)
                      .withBufferSize(0)
                      .withQueue("test"));

          CompletionStage<List<Message>> result =
              jmsSource
                  .take(msgsIn.size())
                  .map(
                      env -> {
                        env.acknowledge();
                        return env.message();
                      })
                  .runWith(Sink.seq(), materializer);

          List<Message> outMessages =
              new ArrayList<>(result.toCompletableFuture().get(3, TimeUnit.SECONDS));
          outMessages.sort(
              (a, b) -> {
                try {
                  return a.getIntProperty("Number") - b.getIntProperty("Number");
                } catch (JMSException e) {
                  throw new RuntimeException(e);
                }
              });

          int msgIdx = 0;
          for (Message outMsg : outMessages) {
            assertEquals(
                outMsg.getIntProperty("Number"),
                msgsIn.get(msgIdx).properties().get("Number").get());
            assertEquals(
                outMsg.getBooleanProperty("IsOdd"),
                msgsIn.get(msgIdx).properties().get("IsOdd").get());
            assertEquals(
                outMsg.getBooleanProperty("IsEven"),
                (msgsIn.get(msgIdx).properties().get("IsEven").get()));
            msgIdx++;
          }
        });
  }

  @Test
  public void publishAndConsumeJmsTextMessagesWithHeaders() throws Exception {
    withServer(
        server -> {
          ConnectionFactory connectionFactory = server.createConnectionFactory();

          Sink<JmsTextMessage, CompletionStage<Done>> jmsSink =
              JmsProducer.sink(
                  JmsProducerSettings.create(producerConfig, connectionFactory).withQueue("test"));

          List<JmsTextMessage> msgsIn =
              createTestMessageList()
                  .stream()
                  .map(jmsTextMessage -> jmsTextMessage.withHeader(JmsType.create("type")))
                  .map(
                      jmsTextMessage ->
                          jmsTextMessage.withHeader(JmsCorrelationId.create("correlationId")))
                  .map(jmsTextMessage -> jmsTextMessage.withHeader(JmsReplyTo.queue("test-reply")))
                  .collect(Collectors.toList());

          Source.from(msgsIn).runWith(jmsSink, materializer);

          Source<AckEnvelope, JmsConsumerControl> jmsSource =
              JmsConsumer.ackSource(
                  JmsConsumerSettings.create(consumerConfig, connectionFactory)
                      .withSessionCount(5)
                      .withBufferSize(0)
                      .withQueue("test"));

          CompletionStage<List<Message>> result =
              jmsSource
                  .take(msgsIn.size())
                  .map(
                      env -> {
                        env.acknowledge();
                        return env.message();
                      })
                  .runWith(Sink.seq(), materializer);

          List<Message> outMessages =
              new ArrayList<>(result.toCompletableFuture().get(3, TimeUnit.SECONDS));
          outMessages.sort(
              (a, b) -> {
                try {
                  return a.getIntProperty("Number") - b.getIntProperty("Number");
                } catch (JMSException e) {
                  throw new RuntimeException(e);
                }
              });
          int msgIdx = 0;
          for (Message outMsg : outMessages) {
            assertEquals(
                outMsg.getIntProperty("Number"),
                msgsIn.get(msgIdx).properties().get("Number").get());
            assertEquals(
                outMsg.getBooleanProperty("IsOdd"),
                msgsIn.get(msgIdx).properties().get("IsOdd").get());
            assertEquals(
                outMsg.getBooleanProperty("IsEven"),
                (msgsIn.get(msgIdx).properties().get("IsEven").get()));
            assertEquals(outMsg.getJMSType(), "type");
            assertEquals(outMsg.getJMSCorrelationID(), "correlationId");
            assertEquals(((ActiveMQQueue) outMsg.getJMSReplyTo()).getQueueName(), "test-reply");
            msgIdx++;
          }
        });
  }

  @Test
  public void publishJmsTextMessagesWithPropertiesAndConsumeThemWithASelector() throws Exception {
    withServer(
        server -> {
          ConnectionFactory connectionFactory = server.createConnectionFactory();

          Sink<JmsTextMessage, CompletionStage<Done>> jmsSink =
              JmsProducer.sink(
                  JmsProducerSettings.create(producerConfig, connectionFactory).withQueue("test"));

          List<JmsTextMessage> msgsIn = createTestMessageList();

          Source.from(msgsIn).runWith(jmsSink, materializer);

          Source<AckEnvelope, JmsConsumerControl> jmsSource =
              JmsConsumer.ackSource(
                  JmsConsumerSettings.create(consumerConfig, connectionFactory)
                      .withSessionCount(5)
                      .withBufferSize(0)
                      .withQueue("test")
                      .withSelector("IsOdd = TRUE"));

          List<JmsTextMessage> oddMsgsIn =
              msgsIn
                  .stream()
                  .filter(msg -> Integer.valueOf(msg.body()) % 2 == 1)
                  .collect(Collectors.toList());
          assertEquals(5, oddMsgsIn.size());

          CompletionStage<List<Message>> result =
              jmsSource
                  .take(oddMsgsIn.size())
                  .map(
                      env -> {
                        env.acknowledge();
                        return env.message();
                      })
                  .runWith(Sink.seq(), materializer);

          List<Message> outMessages =
              new ArrayList<>(result.toCompletableFuture().get(3, TimeUnit.SECONDS));
          outMessages.sort(
              (a, b) -> {
                try {
                  return a.getIntProperty("Number") - b.getIntProperty("Number");
                } catch (JMSException e) {
                  throw new RuntimeException(e);
                }
              });

          int msgIdx = 0;
          for (Message outMsg : outMessages) {
            assertEquals(
                outMsg.getIntProperty("Number"),
                oddMsgsIn.get(msgIdx).properties().get("Number").get());
            assertEquals(
                outMsg.getBooleanProperty("IsOdd"),
                oddMsgsIn.get(msgIdx).properties().get("IsOdd").get());
            assertEquals(
                outMsg.getBooleanProperty("IsEven"),
                (oddMsgsIn.get(msgIdx).properties().get("IsEven").get()));
            assertEquals(1, outMsg.getIntProperty("Number") % 2);
            msgIdx++;
          }
        });
  }

  @Test
  public void publishAndConsumeTopic() throws Exception {
    withServer(
        server -> {
          ConnectionFactory connectionFactory = server.createConnectionFactory();

          List<String> in = Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k");
          List<String> inNumbers =
              IntStream.range(0, 10).boxed().map(String::valueOf).collect(Collectors.toList());

          Sink<String, CompletionStage<Done>> jmsTopicSink =
              JmsProducer.textSink(
                  JmsProducerSettings.create(producerConfig, connectionFactory).withTopic("topic"));
          Sink<String, CompletionStage<Done>> jmsTopicSink2 =
              JmsProducer.textSink(
                  JmsProducerSettings.create(producerConfig, connectionFactory).withTopic("topic"));

          Source<AckEnvelope, JmsConsumerControl> jmsTopicSource =
              JmsConsumer.ackSource(
                  JmsConsumerSettings.create(consumerConfig, connectionFactory)
                      .withSessionCount(1)
                      .withBufferSize(0)
                      .withTopic("topic"));
          Source<AckEnvelope, JmsConsumerControl> jmsTopicSource2 =
              JmsConsumer.ackSource(
                  JmsConsumerSettings.create(consumerConfig, connectionFactory)
                      .withSessionCount(1)
                      .withBufferSize(0)
                      .withTopic("topic"));

          CompletionStage<List<String>> result =
              jmsTopicSource
                  .take(in.size() + inNumbers.size())
                  .map(
                      env -> {
                        env.acknowledge();
                        return ((TextMessage) env.message()).getText();
                      })
                  .runWith(Sink.seq(), materializer)
                  .thenApply(l -> l.stream().sorted().collect(Collectors.toList()));
          CompletionStage<List<String>> result2 =
              jmsTopicSource2
                  .take(in.size() + inNumbers.size())
                  .map(
                      env -> {
                        env.acknowledge();
                        return ((TextMessage) env.message()).getText();
                      })
                  .runWith(Sink.seq(), materializer)
                  .thenApply(l -> l.stream().sorted().collect(Collectors.toList()));

          Thread.sleep(500);

          Source.from(in).runWith(jmsTopicSink, materializer);
          Source.from(inNumbers).runWith(jmsTopicSink2, materializer);

          assertEquals(
              Stream.concat(in.stream(), inNumbers.stream()).sorted().collect(Collectors.toList()),
              result.toCompletableFuture().get(5, TimeUnit.SECONDS));
          assertEquals(
              Stream.concat(in.stream(), inNumbers.stream()).sorted().collect(Collectors.toList()),
              result2.toCompletableFuture().get(5, TimeUnit.SECONDS));
        });
  }

  private void withServer(ConsumerChecked<JmsBroker> test) throws Exception {
    JmsBroker broker = JmsBroker.apply();
    try {
      test.accept(broker);
      Thread.sleep(500);
    } finally {
      broker.stop();
    }
  }

  @FunctionalInterface
  private interface ConsumerChecked<T> {
    void accept(T elt) throws Exception;
  }
}
