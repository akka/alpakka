/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.KillSwitch;
import akka.stream.Materializer;
import akka.stream.alpakka.jms.*;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

final class DummyJavaTests implements java.io.Serializable {

  private final String value;

  DummyJavaTests(String value) {
    this.value = value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o instanceof DummyJavaTests) {
      return ((DummyJavaTests) o).value.equals(this.value);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return value != null ? value.hashCode() : 0;
  }
}

public class JmsConnectorsTest {

  private List<JmsTextMessage> createTestMessageList() {
    List<Integer> intsIn = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    List<JmsTextMessage> msgsIn = new ArrayList<>();
    for (Integer n : intsIn) {

      // #create-messages-with-properties
      JmsTextMessage message =
          JmsTextMessage.create(n.toString())
              .withProperty("Number", n)
              .withProperty("IsOdd", n % 2 == 1)
              .withProperty("IsEven", n % 2 == 0);
      // #create-messages-with-properties

      msgsIn.add(message);
    }

    return msgsIn;
  }

  @Test
  public void publishAndConsumeJmsTextMessage() throws Exception {
    withServer(
        ctx -> {
          // #connection-factory
          javax.jms.ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(ctx.url);
          // #connection-factory

          // #create-text-sink
          Sink<String, CompletionStage<Done>> jmsSink =
              JmsProducer.textSink(JmsProducerSettings.create(connectionFactory).withQueue("test"));
          // #create-text-sink

          // #run-text-sink
          List<String> in = Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k");
          CompletionStage<Done> finished = Source.from(in).runWith(jmsSink, materializer);
          // #run-text-sink

          // #create-text-source
          Source<String, KillSwitch> jmsSource =
              JmsConsumer.textSource(
                  JmsConsumerSettings.create(connectionFactory)
                      .withQueue("test")
                      .withBufferSize(10));
          // #create-text-source

          // #run-text-source
          CompletionStage<List<String>> result =
              jmsSource.take(in.size()).runWith(Sink.seq(), materializer);
          // #run-text-source

          assertEquals(in, result.toCompletableFuture().get(3, TimeUnit.SECONDS));
        });
  }

  @Test
  public void publishAndConsumeJmsObjectMessage() throws Exception {
    withServer(
        ctx -> {
          // #connection-factory-object
          ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(ctx.url);
          connectionFactory.setTrustedPackages(
              Arrays.asList(DummyJavaTests.class.getPackage().getName()));
          // #connection-factory-object

          // #create-object-sink
          Sink<java.io.Serializable, CompletionStage<Done>> jmsSink =
              JmsProducer.objectSink(
                  JmsProducerSettings.create(connectionFactory).withQueue("test"));
          // #create-object-sink

          // #run-object-sink
          java.io.Serializable in = new DummyJavaTests("javaTest");
          CompletionStage<Done> finished = Source.single(in).runWith(jmsSink, materializer);
          // #run-object-sink

          // #create-object-source
          Source<java.io.Serializable, KillSwitch> jmsSource =
              JmsConsumer.objectSource(
                  JmsConsumerSettings.create(connectionFactory).withQueue("test"));
          // #create-object-source

          // #run-object-source
          CompletionStage<java.io.Serializable> result =
              jmsSource.take(1).runWith(Sink.head(), materializer);
          // #run-object-source

          Object resultObject = result.toCompletableFuture().get(3, TimeUnit.SECONDS);
          assertEquals(resultObject, in);
        });
  }

  @Test
  public void publishAndConsumeJmsByteMessage() throws Exception {
    withServer(
        ctx -> {
          ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(ctx.url);

          // #create-bytearray-sink
          Sink<byte[], CompletionStage<Done>> jmsSink =
              JmsProducer.bytesSink(
                  JmsProducerSettings.create(connectionFactory).withQueue("test"));
          // #create-bytearray-sink

          // #run-bytearray-sink
          byte[] in = "ThisIsATest".getBytes(Charset.forName("UTF-8"));
          CompletionStage<Done> finished = Source.single(in).runWith(jmsSink, materializer);
          // #run-bytearray-sink

          // #create-bytearray-source
          Source<byte[], KillSwitch> jmsSource =
              JmsConsumer.bytesSource(
                  JmsConsumerSettings.create(connectionFactory).withQueue("test"));
          // #create-bytearray-source

          // #run-bytearray-source
          CompletionStage<byte[]> result = jmsSource.take(1).runWith(Sink.head(), materializer);
          // #run-bytearray-source

          byte[] resultArray = result.toCompletableFuture().get(3, TimeUnit.SECONDS);
          assertEquals("ThisIsATest", new String(resultArray, Charset.forName("UTF-8")));
        });
  }

  @Test
  public void publishAndConsumeJmsMapMessage() throws Exception {
    withServer(
        ctx -> {
          ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(ctx.url);

          // #create-map-sink
          Sink<Map<String, Object>, CompletionStage<Done>> jmsSink =
              JmsProducer.mapSink(JmsProducerSettings.create(connectionFactory).withQueue("test"));
          // #create-map-sink

          // #run-map-sink
          Map<String, Object> in = new HashMap<>();
          in.put("string value", "value");
          in.put("int value", 42);
          in.put("double value", 43.0);
          in.put("short value", (short) 7);
          in.put("boolean value", true);
          in.put("long value", 7L);
          in.put("bytearray", "AStringAsByteArray".getBytes(Charset.forName("UTF-8")));
          in.put("byte", (byte) 1);

          CompletionStage<Done> finished = Source.single(in).runWith(jmsSink, materializer);
          // #run-map-sink

          // #create-map-source
          Source<Map<String, Object>, KillSwitch> jmsSource =
              JmsConsumer.mapSource(
                  JmsConsumerSettings.create(connectionFactory).withQueue("test"));
          // #create-map-source

          // #run-map-source
          CompletionStage<Map<String, Object>> resultStage =
              jmsSource.take(1).runWith(Sink.head(), materializer);
          // #run-map-source

          Map<String, Object> resultMap =
              resultStage.toCompletableFuture().get(3, TimeUnit.SECONDS);

          assertEquals(resultMap.get("string value"), in.get("string value"));
          assertEquals(resultMap.get("int value"), in.get("int value"));
          assertEquals(resultMap.get("double value"), in.get("double value"));
          assertEquals(resultMap.get("short value"), in.get("short value"));
          assertEquals(resultMap.get("boolean value"), in.get("boolean value"));
          assertEquals(resultMap.get("long value"), in.get("long value"));
          assertEquals(resultMap.get("byte"), in.get("byte"));

          byte[] resultByteArray = (byte[]) resultMap.get("bytearray");
          assertEquals(new String(resultByteArray, Charset.forName("UTF-8")), "AStringAsByteArray");
        });
  }

  @Test
  public void publishAndConsumeJmsTextMessagesWithProperties() throws Exception {
    withServer(
        ctx -> {
          ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(ctx.url);

          // #create-jms-sink
          Sink<JmsTextMessage, CompletionStage<Done>> jmsSink =
              JmsProducer.create(JmsProducerSettings.create(connectionFactory).withQueue("test"));
          // #create-jms-sink

          List<JmsTextMessage> msgsIn = createTestMessageList();

          // #run-jms-sink
          CompletionStage<Done> finished = Source.from(msgsIn).runWith(jmsSink, materializer);
          // #run-jms-sink

          // #create-jms-source
          Source<Message, KillSwitch> jmsSource =
              JmsConsumer.create(
                  JmsConsumerSettings.create(connectionFactory)
                      .withQueue("test")
                      .withBufferSize(10));
          // #create-jms-source

          // #run-jms-source
          CompletionStage<List<Message>> result =
              jmsSource.take(msgsIn.size()).runWith(Sink.seq(), materializer);
          // #run-jms-source

          List<Message> outMessages = result.toCompletableFuture().get(3, TimeUnit.SECONDS);
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
        ctx -> {
          ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(ctx.url);

          Sink<JmsTextMessage, CompletionStage<Done>> jmsSink =
              JmsProducer.create(JmsProducerSettings.create(connectionFactory).withQueue("test"));

          // #create-messages-with-headers
          List<JmsTextMessage> msgsIn =
              createTestMessageList()
                  .stream()
                  .map(
                      jmsTextMessage ->
                          jmsTextMessage
                              .withHeader(JmsType.create("type"))
                              .withHeader(JmsCorrelationId.create("correlationId"))
                              .withHeader(JmsReplyTo.queue("test-reply"))
                              .withHeader(JmsTimeToLive.create(999, TimeUnit.SECONDS))
                              .withHeader(JmsPriority.create(2))
                              .withHeader(JmsDeliveryMode.create(DeliveryMode.NON_PERSISTENT)))
                  .collect(Collectors.toList());
          // #create-messages-with-headers

          Source.from(msgsIn).runWith(jmsSink, materializer);

          Source<Message, KillSwitch> jmsSource =
              JmsConsumer.create(
                  JmsConsumerSettings.create(connectionFactory)
                      .withQueue("test")
                      .withBufferSize(10));

          CompletionStage<List<Message>> result =
              jmsSource.take(msgsIn.size()).runWith(Sink.seq(), materializer);

          List<Message> outMessages = result.toCompletableFuture().get(3, TimeUnit.SECONDS);
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

            assertTrue(outMsg.getJMSExpiration() != 0);
            assertEquals(2, outMsg.getJMSPriority());
            assertEquals(DeliveryMode.NON_PERSISTENT, outMsg.getJMSDeliveryMode());
            msgIdx++;
          }
        });
  }

  @Test
  public void publishJmsTextMessagesWithPropertiesAndConsumeThemWithASelector() throws Exception {
    withServer(
        ctx -> {
          ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(ctx.url);

          Sink<JmsTextMessage, CompletionStage<Done>> jmsSink =
              JmsProducer.create(JmsProducerSettings.create(connectionFactory).withQueue("test"));

          List<JmsTextMessage> msgsIn = createTestMessageList();

          Source.from(msgsIn).runWith(jmsSink, materializer);

          // #create-jms-source-with-selector
          Source<Message, KillSwitch> jmsSource =
              JmsConsumer.create(
                  JmsConsumerSettings.create(connectionFactory)
                      .withQueue("test")
                      .withBufferSize(10)
                      .withSelector("IsOdd = TRUE"));
          // #create-jms-source-with-selector

          // #assert-only-odd-messages-received
          List<JmsTextMessage> oddMsgsIn =
              msgsIn
                  .stream()
                  .filter(msg -> Integer.valueOf(msg.body()) % 2 == 1)
                  .collect(Collectors.toList());
          assertEquals(5, oddMsgsIn.size());

          CompletionStage<List<Message>> result =
              jmsSource.take(oddMsgsIn.size()).runWith(Sink.seq(), materializer);

          List<Message> outMessages = result.toCompletableFuture().get(4, TimeUnit.SECONDS);
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
          // #assert-only-odd-messages-received
        });
  }

  @Test
  public void publishAndConsumeTopic() throws Exception {
    withServer(
        ctx -> {
          ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(ctx.url);

          List<String> in = Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k");
          List<String> inNumbers =
              IntStream.range(0, 10).boxed().map(String::valueOf).collect(Collectors.toList());

          // #create-topic-sink
          Sink<String, CompletionStage<Done>> jmsTopicSink =
              JmsProducer.textSink(
                  JmsProducerSettings.create(connectionFactory).withTopic("topic"));
          // #create-topic-sink
          Sink<String, CompletionStage<Done>> jmsTopicSink2 =
              JmsProducer.textSink(
                  JmsProducerSettings.create(connectionFactory).withTopic("topic"));

          // #create-topic-source
          Source<String, KillSwitch> jmsTopicSource =
              JmsConsumer.textSource(
                  JmsConsumerSettings.create(connectionFactory)
                      .withTopic("topic")
                      .withBufferSize(10));
          // #create-topic-source
          Source<String, KillSwitch> jmsTopicSource2 =
              JmsConsumer.textSource(
                  JmsConsumerSettings.create(connectionFactory)
                      .withTopic("topic")
                      .withBufferSize(10));

          // #run-topic-source
          CompletionStage<List<String>> result =
              jmsTopicSource
                  .take(in.size() + inNumbers.size())
                  .runWith(Sink.seq(), materializer)
                  .thenApply(l -> l.stream().sorted().collect(Collectors.toList()));
          // #run-topic-source
          CompletionStage<List<String>> result2 =
              jmsTopicSource2
                  .take(in.size() + inNumbers.size())
                  .runWith(Sink.seq(), materializer)
                  .thenApply(l -> l.stream().sorted().collect(Collectors.toList()));

          Thread.sleep(500);

          // #run-topic-sink
          CompletionStage<Done> finished = Source.from(in).runWith(jmsTopicSink, materializer);
          // #run-topic-sink
          Source.from(inNumbers).runWith(jmsTopicSink2, materializer);

          assertEquals(
              Stream.concat(in.stream(), inNumbers.stream()).sorted().collect(Collectors.toList()),
              result.toCompletableFuture().get(5, TimeUnit.SECONDS));
          assertEquals(
              Stream.concat(in.stream(), inNumbers.stream()).sorted().collect(Collectors.toList()),
              result2.toCompletableFuture().get(5, TimeUnit.SECONDS));
        });
  }

  @Test
  public void publishAndConsumeJmsTextMessagesWithClientAcknowledgement() throws Exception {
    withServer(
        ctx -> {
          ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(ctx.url);

          Sink<JmsTextMessage, CompletionStage<Done>> jmsSink =
              JmsProducer.create(JmsProducerSettings.create(connectionFactory).withQueue("test"));
          List<Integer> intsIn = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
          List<JmsTextMessage> msgsIn = new ArrayList<>();
          for (Integer n : intsIn) {
            msgsIn.add(JmsTextMessage.create(n.toString()));
          }

          Source.from(msgsIn).runWith(jmsSink, materializer);

          // #create-jms-source-client-ack
          Source<Message, KillSwitch> jmsSource =
              JmsConsumer.create(
                  JmsConsumerSettings.create(connectionFactory)
                      .withQueue("test")
                      .withAcknowledgeMode(AcknowledgeMode.ClientAcknowledge()));
          // #create-jms-source-client-ack

          // #run-jms-source-with-ack
          CompletionStage<List<String>> result =
              jmsSource
                  .map(
                      message -> {
                        String text = ((ActiveMQTextMessage) message).getText();
                        message.acknowledge();
                        return text;
                      })
                  .take(msgsIn.size())
                  .runWith(Sink.seq(), materializer);
          // #run-jms-source-with-ack

          List<String> outMessages = result.toCompletableFuture().get(3, TimeUnit.SECONDS);
          int msgIdx = 0;
          for (String outMsg : outMessages) {
            assertEquals(outMsg, msgsIn.get(msgIdx).body());
            msgIdx++;
          }
        });
  }

  @Test
  public void sinkNormalCompletion() throws Exception {
    withServer(
        ctx -> {
          ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(ctx.url);

          Sink<JmsTextMessage, CompletionStage<Done>> jmsSink =
              JmsProducer.create(JmsProducerSettings.create(connectionFactory).withQueue("test"));

          List<JmsTextMessage> msgsIn = createTestMessageList();

          CompletionStage<Done> completionFuture =
              Source.from(msgsIn).runWith(jmsSink, materializer);
          Done completed = completionFuture.toCompletableFuture().get(3, TimeUnit.SECONDS);
          assertEquals(completed, Done.getInstance());
        });
  }

  @Test
  public void sinkExceptionalCompletion() throws Exception {
    withServer(
        ctx -> {
          ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(ctx.url);

          Sink<JmsTextMessage, CompletionStage<Done>> jmsSink =
              JmsProducer.create(JmsProducerSettings.create(connectionFactory).withQueue("test"));

          CompletionStage<Done> completionFuture =
              Source.<JmsTextMessage>failed(new RuntimeException("Simulated error"))
                  .runWith(jmsSink, materializer);

          try {
            completionFuture.toCompletableFuture().get(3, TimeUnit.SECONDS);
            fail("Not completed exceptionally");
          } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            assertTrue(cause instanceof RuntimeException);
          }
        });
  }

  @Test
  public void sinkExceptionalCompletionOnDisconnect() throws Exception {
    withServer(
        ctx -> {
          ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(ctx.url);

          Sink<JmsTextMessage, CompletionStage<Done>> jmsSink =
              JmsProducer.create(JmsProducerSettings.create(connectionFactory).withQueue("test"));

          List<JmsTextMessage> msgsIn = createTestMessageList();

          CompletionStage<Done> completionFuture =
              Source.from(msgsIn)
                  .mapAsync(
                      1,
                      m ->
                          CompletableFuture.supplyAsync(
                              () -> {
                                try {
                                  Thread.sleep(500);
                                } catch (InterruptedException e) {
                                  throw new RuntimeException(e);
                                }
                                return m;
                              }))
                  .runWith(jmsSink, materializer);

          ctx.broker.stop();

          try {
            completionFuture.toCompletableFuture().get(3, TimeUnit.SECONDS);
            fail("Not completed exceptionally");
          } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            assertTrue(cause instanceof JMSException);
          }
        });
  }

  @Test
  public void browse() throws Exception {
    withServer(
        ctx -> {
          ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(ctx.url);

          Sink<String, CompletionStage<Done>> jmsSink =
              JmsProducer.textSink(JmsProducerSettings.create(connectionFactory).withQueue("test"));

          List<String> in = Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k");

          Source.from(in).runWith(jmsSink, materializer).toCompletableFuture().get();

          // #create-browse-source
          Source<Message, NotUsed> browseSource =
              JmsConsumer.browse(JmsBrowseSettings.create(connectionFactory).withQueue("test"));
          // #create-browse-source

          // #run-browse-source
          CompletionStage<List<Message>> result = browseSource.runWith(Sink.seq(), materializer);
          // #run-browse-source

          List<String> resultText =
              result
                  .toCompletableFuture()
                  .get()
                  .stream()
                  .map(
                      message -> {
                        try {
                          return ((ActiveMQTextMessage) message).getText();
                        } catch (JMSException e) {
                          throw new RuntimeException(e);
                        }
                      })
                  .collect(Collectors.toList());

          assertEquals(in, resultText);
        });
  }

  @Test
  public void producerFlow() throws Exception {
    withServer(
        ctx -> {
          ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(ctx.url);

          // #create-flow-producer
          Flow<JmsTextMessage, JmsTextMessage, NotUsed> flowSink =
              JmsProducer.flow(JmsProducerSettings.create(connectionFactory).withQueue("test"));
          // #create-flow-producer

          // #run-flow-producer
          List<JmsTextMessage> input = createTestMessageList();

          CompletionStage<List<JmsTextMessage>> result =
              Source.from(input).via(flowSink).runWith(Sink.seq(), materializer);
          // #run-flow-producer

          assertEquals(input, result.toCompletableFuture().get());
        });
  }

  private static ActorSystem system;
  private static Materializer materializer;

  @BeforeClass
  public static void setup() {
    system = ActorSystem.create();
    materializer = ActorMaterializer.create(system);
  }

  @AfterClass
  public static void teardown() {
    TestKit.shutdownActorSystem(system);
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
      Thread.sleep(100);
    } finally {
      if (broker.isStarted()) {
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
