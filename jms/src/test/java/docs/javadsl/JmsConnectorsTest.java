/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.jms.*;
import akka.stream.alpakka.jms.javadsl.JmsConsumer;
import akka.stream.alpakka.jms.javadsl.JmsConsumerControl;
import akka.stream.alpakka.jms.javadsl.JmsProducer;
import akka.stream.alpakka.jms.javadsl.JmsProducerStatus;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;
import jmstestkit.JmsBroker;
import com.typesafe.config.Config;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.util.Failure;
import scala.util.Success;
import scala.util.Try;

import javax.jms.*;
import javax.jms.Message;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
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

  private static ActorSystem system;
  private static Materializer materializer;
  private static Config producerConfig;
  private static Config browseConfig;

  @BeforeClass
  public static void setup() {
    system = ActorSystem.create();
    materializer = ActorMaterializer.create(system);
    producerConfig = system.settings().config().getConfig(JmsProducerSettings.configPath());
    browseConfig = system.settings().config().getConfig(JmsBrowseSettings.configPath());
  }

  @AfterClass
  public static void teardown() {
    TestKit.shutdownActorSystem(system);
  }

  private List<JmsTextMessage> createTestMessageList() {
    List<Integer> intsIn = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    List<JmsTextMessage> msgsIn = new ArrayList<>();
    for (Integer n : intsIn) {

      // #create-messages-with-properties
      JmsTextMessage message =
          akka.stream.alpakka.jms.JmsTextMessage.create(n.toString())
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
        server -> {
          // #connection-factory #text-sink
          // #text-source
          javax.jms.ConnectionFactory connectionFactory = server.createConnectionFactory();
          // #text-source
          // #connection-factory #text-sink

          // #text-sink

          Sink<String, CompletionStage<Done>> jmsSink =
              JmsProducer.textSink(
                  JmsProducerSettings.create(system, connectionFactory).withQueue("test"));

          List<String> in = Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k");
          CompletionStage<Done> finished = Source.from(in).runWith(jmsSink, materializer);
          // #text-sink

          // #text-source
          Source<String, JmsConsumerControl> jmsSource =
              JmsConsumer.textSource(
                  JmsConsumerSettings.create(system, connectionFactory).withQueue("test"));

          CompletionStage<List<String>> result =
              jmsSource.take(in.size()).runWith(Sink.seq(), materializer);
          // #text-source

          assertEquals(Done.getInstance(), finished.toCompletableFuture().get(3, TimeUnit.SECONDS));
          assertEquals(in, result.toCompletableFuture().get(3, TimeUnit.SECONDS));
        });
  }

  @Test
  public void publishAndConsumeJmsObjectMessage() throws Exception {
    withServer(
        server -> {
          // #connection-factory-object #object-sink
          // #object-source
          ActiveMQConnectionFactory connectionFactory =
              (ActiveMQConnectionFactory) server.createConnectionFactory();
          connectionFactory.setTrustedPackages(
              Arrays.asList(DummyJavaTests.class.getPackage().getName()));

          // #object-source
          // #connection-factory-object #object-sink

          // #object-sink
          Sink<java.io.Serializable, CompletionStage<Done>> jmsSink =
              JmsProducer.objectSink(
                  JmsProducerSettings.create(system, connectionFactory).withQueue("test"));

          java.io.Serializable in = new DummyJavaTests("javaTest");
          CompletionStage<Done> finished = Source.single(in).runWith(jmsSink, materializer);
          // #object-sink

          // #object-source
          Source<java.io.Serializable, JmsConsumerControl> jmsSource =
              JmsConsumer.objectSource(
                  JmsConsumerSettings.create(system, connectionFactory).withQueue("test"));

          CompletionStage<java.io.Serializable> result =
              jmsSource.take(1).runWith(Sink.head(), materializer);
          // #object-source

          assertEquals(Done.getInstance(), finished.toCompletableFuture().get(3, TimeUnit.SECONDS));
          Object resultObject = result.toCompletableFuture().get(3, TimeUnit.SECONDS);
          assertEquals(resultObject, in);
        });
  }

  @Test
  public void publishAndConsumeJmsByteMessage() throws Exception {
    withServer(
        server -> {
          // #bytearray-sink #bytearray-source
          ConnectionFactory connectionFactory = server.createConnectionFactory();

          // #bytearray-sink #bytearray-source

          // #bytearray-sink
          Sink<byte[], CompletionStage<Done>> jmsSink =
              JmsProducer.bytesSink(
                  JmsProducerSettings.create(producerConfig, connectionFactory).withQueue("test"));

          byte[] in = "ThisIsATest".getBytes(Charset.forName("UTF-8"));
          CompletionStage<Done> finished = Source.single(in).runWith(jmsSink, materializer);
          // #bytearray-sink

          // #bytearray-source
          Source<byte[], JmsConsumerControl> jmsSource =
              JmsConsumer.bytesSource(
                  JmsConsumerSettings.create(system, connectionFactory).withQueue("test"));

          CompletionStage<byte[]> result = jmsSource.take(1).runWith(Sink.head(), materializer);
          // #bytearray-source

          assertEquals(Done.getInstance(), finished.toCompletableFuture().get(3, TimeUnit.SECONDS));
          byte[] resultArray = result.toCompletableFuture().get(3, TimeUnit.SECONDS);
          assertEquals("ThisIsATest", new String(resultArray, Charset.forName("UTF-8")));
        });
  }

  @Test
  public void publishAndConsumeJmsMapMessage() throws Exception {
    withServer(
        server -> {
          // #map-sink #map-source
          ConnectionFactory connectionFactory = server.createConnectionFactory();

          // #map-sink #map-source
          // #map-sink
          Sink<Map<String, Object>, CompletionStage<Done>> jmsSink =
              JmsProducer.mapSink(
                  JmsProducerSettings.create(system, connectionFactory).withQueue("test"));

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
          // #map-sink

          // #map-source
          Source<Map<String, Object>, JmsConsumerControl> jmsSource =
              JmsConsumer.mapSource(
                  JmsConsumerSettings.create(system, connectionFactory).withQueue("test"));

          CompletionStage<Map<String, Object>> resultStage =
              jmsSource.take(1).runWith(Sink.head(), materializer);
          // #map-source

          Map<String, Object> resultMap =
              resultStage.toCompletableFuture().get(3, TimeUnit.SECONDS);

          assertEquals(resultMap.get("string value"), in.get("string value"));
          assertEquals(resultMap.get("int value"), in.get("int value"));
          assertEquals(resultMap.get("double value"), in.get("double value"));
          assertEquals(resultMap.get("short value"), in.get("short value"));
          assertEquals(resultMap.get("boolean value"), in.get("boolean value"));
          assertEquals(resultMap.get("long value"), in.get("long value"));
          assertEquals(resultMap.get("byte"), in.get("byte"));

          assertEquals(Done.getInstance(), finished.toCompletableFuture().get(3, TimeUnit.SECONDS));
          byte[] resultByteArray = (byte[]) resultMap.get("bytearray");
          assertEquals(new String(resultByteArray, Charset.forName("UTF-8")), "AStringAsByteArray");
        });
  }

  @Test
  public void publishAndConsumeJmsTextMessagesWithProperties() throws Exception {
    withServer(
        server -> {
          // #jms-source
          ConnectionFactory connectionFactory = server.createConnectionFactory();

          // #jms-source

          int expectedMessages = 2;

          // #create-jms-sink
          Sink<JmsTextMessage, CompletionStage<Done>> jmsSink =
              JmsProducer.sink(
                  JmsProducerSettings.create(producerConfig, connectionFactory).withQueue("test"));

          CompletionStage<Done> finished =
              Source.from(Arrays.asList("Message A", "Message B"))
                  .map(JmsTextMessage::create)
                  .runWith(jmsSink, materializer);
          // #create-jms-sink

          // #jms-source
          Source<javax.jms.Message, JmsConsumerControl> jmsSource =
              JmsConsumer.create(
                  JmsConsumerSettings.create(system, connectionFactory).withQueue("test"));

          Pair<JmsConsumerControl, CompletionStage<List<String>>> controlAndResult =
              jmsSource
                  .take(expectedMessages)
                  .map(
                      msg -> {
                        if (msg instanceof TextMessage) {
                          TextMessage t = (TextMessage) msg;
                          return t.getText();
                        } else
                          throw new RuntimeException("unexpected message type " + msg.getClass());
                      })
                  .toMat(Sink.seq(), Keep.both())
                  .run(materializer);

          // #jms-source

          CompletionStage<List<String>> result = controlAndResult.second();
          List<String> outMessages = result.toCompletableFuture().get(3, TimeUnit.SECONDS);
          assertEquals("unexpected number of elements", expectedMessages, outMessages.size());
          // #jms-source
          JmsConsumerControl control = controlAndResult.first();
          control.shutdown();
          // #jms-source
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

          // #create-messages-with-headers
          List<JmsTextMessage> msgsIn =
              createTestMessageList().stream()
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

          Source<Message, JmsConsumerControl> jmsSource =
              JmsConsumer.create(
                  JmsConsumerSettings.create(system, connectionFactory).withQueue("test"));

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

  // #custom-destination
  Function<javax.jms.Session, javax.jms.Destination> createQueue(String destinationName) {
    return (session) -> {
      ActiveMQSession amqSession = (ActiveMQSession) session;
      try {
        return amqSession.createQueue("my-" + destinationName);
      } catch (JMSException e) {
        throw new RuntimeException(e);
      }
    };
  }
  // #custom-destination

  @Test
  public void useCustomDesination() throws Exception {
    withServer(
        server -> {
          ConnectionFactory connectionFactory = server.createConnectionFactory();
          Sink<JmsTextMessage, CompletionStage<Done>> jmsSink =
              JmsProducer.sink(
                  JmsProducerSettings.create(producerConfig, connectionFactory)
                      .withDestination(new CustomDestination("custom", createQueue("custom"))));

          List<JmsTextMessage> msgsIn = createTestMessageList();

          Source.from(msgsIn).runWith(jmsSink, materializer);
          // #custom-destination

          Source<Message, JmsConsumerControl> jmsSource =
              JmsConsumer.create(
                  JmsConsumerSettings.create(system, connectionFactory)
                      .withDestination(new CustomDestination("custom", createQueue("custom"))));
          // #custom-destination

          CompletionStage<List<Message>> result =
              jmsSource.take(msgsIn.size()).runWith(Sink.seq(), materializer);
          List<Message> outMessages = result.toCompletableFuture().get(3, TimeUnit.SECONDS);
          assertEquals(10, outMessages.size());
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

          // #source-with-selector
          Source<Message, JmsConsumerControl> jmsSource =
              JmsConsumer.create(
                  JmsConsumerSettings.create(system, connectionFactory)
                      .withQueue("test")
                      .withSelector("IsOdd = TRUE"));
          // #source-with-selector

          List<JmsTextMessage> oddMsgsIn =
              msgsIn.stream()
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

          Source<String, JmsConsumerControl> jmsTopicSource =
              JmsConsumer.textSource(
                  JmsConsumerSettings.create(system, connectionFactory).withTopic("topic"));

          Source<String, JmsConsumerControl> jmsTopicSource2 =
              JmsConsumer.textSource(
                  JmsConsumerSettings.create(system, connectionFactory).withTopic("topic"));

          CompletionStage<List<String>> result =
              jmsTopicSource
                  .take(in.size() + inNumbers.size())
                  .runWith(Sink.seq(), materializer)
                  .thenApply(l -> l.stream().sorted().collect(Collectors.toList()));

          CompletionStage<List<String>> result2 =
              jmsTopicSource2
                  .take(in.size() + inNumbers.size())
                  .runWith(Sink.seq(), materializer)
                  .thenApply(l -> l.stream().sorted().collect(Collectors.toList()));

          Thread.sleep(500);

          CompletionStage<Done> finished = Source.from(in).runWith(jmsTopicSink, materializer);
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
  public void sinkNormalCompletion() throws Exception {
    withServer(
        server -> {
          ConnectionFactory connectionFactory = server.createConnectionFactory();

          Sink<JmsTextMessage, CompletionStage<Done>> jmsSink =
              JmsProducer.sink(
                  JmsProducerSettings.create(producerConfig, connectionFactory)
                      .withQueue("test")
                      .withConnectionRetrySettings(
                          ConnectionRetrySettings.create(system).withMaxRetries(0)));

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
        server -> {
          ConnectionFactory connectionFactory = server.createConnectionFactory();

          Sink<JmsTextMessage, CompletionStage<Done>> jmsSink =
              JmsProducer.sink(
                  JmsProducerSettings.create(producerConfig, connectionFactory).withQueue("test"));

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
        server -> {
          ConnectionFactory connectionFactory = server.createConnectionFactory();

          Sink<JmsTextMessage, CompletionStage<Done>> jmsSink =
              JmsProducer.sink(
                  JmsProducerSettings.create(producerConfig, connectionFactory)
                      .withQueue("test")
                      .withConnectionRetrySettings(
                          ConnectionRetrySettings.create(system).withMaxRetries(0)));

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

          try { // Make sure connection got started before stopping.
            Thread.sleep(500);
          } catch (InterruptedException e) {
            fail("Sleep interrupted.");
          }

          server.stop();

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
        server -> {
          // #browse-source
          ConnectionFactory connectionFactory = server.createConnectionFactory();

          // #browse-source

          Sink<String, CompletionStage<Done>> jmsSink =
              JmsProducer.textSink(
                  JmsProducerSettings.create(producerConfig, connectionFactory).withQueue("test"));

          List<String> in = Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k");

          Source.from(in).runWith(jmsSink, materializer).toCompletableFuture().get();

          // #browse-source
          Source<javax.jms.Message, NotUsed> browseSource =
              JmsConsumer.browse(
                  JmsBrowseSettings.create(system, connectionFactory).withQueue("test"));

          CompletionStage<List<javax.jms.Message>> result =
              browseSource.runWith(Sink.seq(), materializer);
          // #browse-source

          List<String> resultText =
              result.toCompletableFuture().get().stream()
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
  public void publishAndConsumeDurableTopic() throws Exception {
    withServer(
        server -> {
          ConnectionFactory producerConnectionFactory = server.createConnectionFactory();
          // #create-connection-factory-with-client-id
          ConnectionFactory consumerConnectionFactory = server.createConnectionFactory();
          ((ActiveMQConnectionFactory) consumerConnectionFactory)
              .setClientID(getClass().getSimpleName());
          // #create-connection-factory-with-client-id

          List<String> in = Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k");

          Sink<String, CompletionStage<Done>> jmsTopicSink =
              JmsProducer.textSink(
                  JmsProducerSettings.create(producerConfig, producerConnectionFactory)
                      .withTopic("topic"));

          // #create-durable-topic-source
          Source<String, JmsConsumerControl> jmsTopicSource =
              JmsConsumer.textSource(
                  JmsConsumerSettings.create(system, consumerConnectionFactory)
                      .withDurableTopic("topic", "durable-test"));
          // #create-durable-topic-source

          // #run-durable-topic-source
          CompletionStage<List<String>> result =
              jmsTopicSource.take(in.size()).runWith(Sink.seq(), materializer);
          // #run-durable-topic-source

          Thread.sleep(500);

          Source.from(in).runWith(jmsTopicSink, materializer);

          assertEquals(in, result.toCompletableFuture().get(5, TimeUnit.SECONDS));
        });
  }

  @Test
  public void producerFlow() throws Exception {
    withServer(
        server -> {
          // #flow-producer
          ConnectionFactory connectionFactory = server.createConnectionFactory();

          Flow<JmsTextMessage, JmsTextMessage, JmsProducerStatus> flow =
              JmsProducer.flow(
                  JmsProducerSettings.create(system, connectionFactory).withQueue("test"));

          List<JmsTextMessage> input = createTestMessageList();

          CompletionStage<List<JmsTextMessage>> result =
              Source.from(input).via(flow).runWith(Sink.seq(), materializer);
          // #flow-producer

          assertEquals(input, result.toCompletableFuture().get());
        });
  }

  @Test
  public void directedProducerFlow() throws Exception {
    withServer(
        server -> {
          ConnectionFactory connectionFactory = server.createConnectionFactory();

          // #run-directed-flow-producer
          Flow<JmsTextMessage, JmsTextMessage, JmsProducerStatus> flowSink =
              JmsProducer.flow(
                  JmsProducerSettings.create(system, connectionFactory).withQueue("test"));

          List<JmsTextMessage> input = new ArrayList<>();
          for (Integer n : Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)) {
            String queueName = (n % 2 == 0) ? "even" : "odd";
            input.add(JmsTextMessage.create(n.toString()).toQueue(queueName));
          }

          Source.from(input).via(flowSink).runWith(Sink.seq(), materializer);
          // #run-directed-flow-producer

          CompletionStage<List<Integer>> even =
              JmsConsumer.textSource(
                      JmsConsumerSettings.create(system, connectionFactory).withQueue("even"))
                  .take(5)
                  .map(Integer::parseInt)
                  .runWith(Sink.seq(), materializer);

          CompletionStage<List<Integer>> odd =
              JmsConsumer.textSource(
                      JmsConsumerSettings.create(system, connectionFactory).withQueue("odd"))
                  .take(5)
                  .map(Integer::parseInt)
                  .runWith(Sink.seq(), materializer);

          assertEquals(Arrays.asList(1, 3, 5, 7, 9), odd.toCompletableFuture().get());
          assertEquals(Arrays.asList(2, 4, 6, 8, 10), even.toCompletableFuture().get());
        });
  }

  @Test
  public void failAfterRetry() throws Exception {
    withServer(
        server -> {
          ConnectionFactory connectionFactory = server.createConnectionFactory();
          server.stop();
          long startTime = System.currentTimeMillis();
          CompletionStage<List<Message>> result =
              JmsConsumer.create(
                      JmsConsumerSettings.create(system, connectionFactory)
                          .withConnectionRetrySettings(
                              ConnectionRetrySettings.create(system).withMaxRetries(4))
                          .withQueue("test"))
                  .runWith(Sink.seq(), materializer);

          CompletionStage<Try<List<Message>>> tryFuture =
              result.handle(
                  (l, e) -> {
                    if (l != null) return Success.apply(l);
                    else return Failure.apply(e);
                  });

          Try<List<Message>> tryResult = tryFuture.toCompletableFuture().get();
          long endTime = System.currentTimeMillis();

          assertTrue("Total retry is too short", endTime - startTime > 100L + 400L + 900L + 1600L);
          assertTrue("Result must be a failure", tryResult.isFailure());
          Throwable exception = tryResult.failed().get();
          assertTrue(
              "Did not fail with a ConnectionRetryException",
              exception instanceof ConnectionRetryException);
          assertTrue(
              "Cause of failure is not a JMSException",
              exception.getCause() instanceof JMSException);
        });
  }

  @Test
  public void passThroughMessageEnvelopes() throws Exception {
    withServer(
        server -> {
          ConnectionFactory connectionFactory = server.createConnectionFactory();
          // #run-flexi-flow-producer
          Flow<JmsEnvelope<String>, JmsEnvelope<String>, JmsProducerStatus> jmsProducer =
              JmsProducer.flexiFlow(
                  JmsProducerSettings.create(producerConfig, connectionFactory).withQueue("test"));

          List<String> data = Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k");
          List<JmsEnvelope<String>> input = new ArrayList<>();
          for (String s : data) {
            String passThrough = s;
            input.add(JmsTextMessage.create(s, passThrough));
          }

          CompletionStage<List<String>> result =
              Source.from(input)
                  .via(jmsProducer)
                  .map(JmsEnvelope::passThrough)
                  .runWith(Sink.seq(), materializer);
          // #run-flexi-flow-producer
          assertEquals(data, result.toCompletableFuture().get());
        });
  }

  @Test
  public void passThroughEmptyMessageEnvelopes() throws Exception {
    withServer(
        server -> {
          ConnectionFactory connectionFactory = server.createConnectionFactory();

          Pair<JmsConsumerControl, CompletionStage<List<String>>> switchAndItems =
              JmsConsumer.textSource(
                      JmsConsumerSettings.create(system, connectionFactory).withQueue("test"))
                  .toMat(Sink.seq(), Keep.both())
                  .run(materializer);

          // #run-flexi-flow-pass-through-producer
          Flow<JmsEnvelope<String>, JmsEnvelope<String>, JmsProducerStatus> jmsProducer =
              JmsProducer.flexiFlow(
                  JmsProducerSettings.create(producerConfig, connectionFactory).withQueue("test"));

          List<String> data = Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k");
          List<JmsEnvelope<String>> input = new ArrayList<>();
          for (String s : data) {
            String passThrough = s;
            input.add(JmsPassThrough.create(passThrough));
          }

          CompletionStage<List<String>> result =
              Source.from(input)
                  .via(jmsProducer)
                  .map(JmsEnvelope::passThrough)
                  .runWith(Sink.seq(), materializer);
          // #run-flexi-flow-pass-through-producer

          assertEquals(data, result.toCompletableFuture().get());

          Thread.sleep(500);

          switchAndItems.first().shutdown();
          assertTrue(switchAndItems.second().toCompletableFuture().get().isEmpty());
        });
  }

  private void withServer(ConsumerChecked<JmsBroker> test) throws Exception {
    JmsBroker broker = JmsBroker.apply();
    try {
      test.accept(broker);
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
}
