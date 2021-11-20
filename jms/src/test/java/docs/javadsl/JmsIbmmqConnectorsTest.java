/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.actor.ActorSystem;
import akka.stream.alpakka.jms.CustomDestination;
import akka.stream.alpakka.jms.JmsConsumerSettings;
import akka.stream.alpakka.jms.JmsProducerSettings;
import akka.stream.alpakka.jms.TxEnvelope;
import akka.stream.alpakka.jms.javadsl.JmsConsumer;
import akka.stream.alpakka.jms.javadsl.JmsConsumerControl;
import akka.stream.alpakka.jms.javadsl.JmsProducer;
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;
import com.ibm.mq.jms.MQConnectionFactory;
import com.ibm.mq.jms.MQQueueConnectionFactory;
import com.ibm.mq.jms.MQQueueSession;
import com.ibm.mq.jms.MQTopicConnectionFactory;
import com.ibm.msg.client.wmq.common.CommonConstants;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;

public class JmsIbmmqConnectorsTest {

  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  private static ActorSystem system;
  private static MQQueueConnectionFactory queueConnectionFactory;
  private static MQTopicConnectionFactory topicConnectionFactory;

  @BeforeClass
  public static void setup() throws JMSException {
    system = ActorSystem.create();
    // #ibmmq-connection-factory
    // Create the IBM MQ MQQueueConnectionFactory
    MQQueueConnectionFactory connectionFactory = new MQQueueConnectionFactory();

    // #ibmmq-connection-factory
    JmsIbmmqConnectorsTest.queueConnectionFactory =
        (MQQueueConnectionFactory) initDefaultFactory(connectionFactory);
    JmsIbmmqConnectorsTest.topicConnectionFactory =
        (MQTopicConnectionFactory) initDefaultFactory(new MQTopicConnectionFactory());
  }

  private static MQConnectionFactory initDefaultFactory(MQConnectionFactory connectionFactory)
      throws JMSException {
    // #ibmmq-connection-factory
    // align to docker image: ibmcom/mq:9.1.1.0
    connectionFactory.setHostName("localhost");
    connectionFactory.setPort(1414);
    connectionFactory.setQueueManager("QM1");
    connectionFactory.setChannel("DEV.APP.SVRCONN");

    // #ibmmq-connection-factory
    return connectionFactory;
  }

  @AfterClass
  public static void teardown() {
    TestKit.shutdownActorSystem(system);
  }

  @Test
  public void publishAndConsumeQueue()
      throws JMSException, InterruptedException, ExecutionException, TimeoutException {

    // #ibmmq-queue
    // Connect to IBM MQ over TCP/IP
    queueConnectionFactory.setTransportType(CommonConstants.WMQ_CM_CLIENT);
    String queueName = "DEV.QUEUE.1";

    Sink<String, CompletionStage<Done>> jmsSink =
        JmsProducer.textSink(
            JmsProducerSettings.create(system, queueConnectionFactory).withQueue(queueName));

    // #ibmmq-queue

    List<String> in = Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k");
    Source.from(in).runWith(jmsSink, system);

    // #ibmmq-queue
    // Option1: create Source using default factory with just name
    Source<TxEnvelope, JmsConsumerControl> txJmsSource =
        JmsConsumer.txSource(
            JmsConsumerSettings.create(system, queueConnectionFactory).withQueue(queueName));

    // #ibmmq-queue

    CompletionStage<List<String>> result =
        txJmsSource
            .take(in.size())
            .map(
                envelope -> {
                  envelope.commit();
                  if (envelope.message() instanceof TextMessage) {
                    TextMessage message = (TextMessage) envelope.message();
                    return message.getText();
                  } else {
                    throw new RuntimeException(
                        "unexpected message type " + envelope.message().getClass());
                  }
                })
            .runWith(Sink.seq(), system);

    List<String> out = new ArrayList<>(result.toCompletableFuture().get(3, TimeUnit.SECONDS));
    Collections.sort(out);
    assertEquals(in, out);
  }

  @Test
  public void publishAndConsumeCustomDestination()
      throws JMSException, InterruptedException, ExecutionException, TimeoutException {

    queueConnectionFactory.setTransportType(CommonConstants.WMQ_CM_CLIENT);

    // #ibmmq-custom-destination
    // Option2: create Source using custom factory
    String customQueue = "DEV.QUEUE.3";
    // #ibmmq-custom-destination

    // "app" clients are limited to perform create queue
    // existing queue in use
    Sink<String, CompletionStage<Done>> jmsSink =
        JmsProducer.textSink(
            JmsProducerSettings.create(system, queueConnectionFactory)
                .withDestination(new CustomDestination("custom", createQueue(customQueue))));

    List<String> in = Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k");

    Source.from(in).runWith(jmsSink, system);

    // #ibmmq-custom-destination
    Source<String, JmsConsumerControl> jmsSource =
        JmsConsumer.textSource(
            JmsConsumerSettings.create(system, queueConnectionFactory)
                .withDestination(new CustomDestination("custom", createQueue(customQueue))));

    // #ibmmq-custom-destination

    CompletionStage<List<String>> result = jmsSource.take(in.size()).runWith(Sink.seq(), system);

    assertEquals(in.size(), result.toCompletableFuture().get(5, TimeUnit.SECONDS).size());
  }

  @Test
  public void publishAndConsumeTopic()
      throws JMSException, InterruptedException, ExecutionException, TimeoutException {

    List<String> in = Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k");

    // #ibmmq-topic
    // Connect to IBM MQ over TCP/IP
    topicConnectionFactory.setTransportType(CommonConstants.WMQ_CM_CLIENT);
    String testTopicName = "dev/";

    Sink<String, CompletionStage<Done>> jmsTopicSink =
        JmsProducer.textSink(
            JmsProducerSettings.create(system, topicConnectionFactory).withTopic(testTopicName));

    // Option1: create Source using default factory with just name
    Source<String, JmsConsumerControl> jmsTopicSource =
        JmsConsumer.textSource(
            JmsConsumerSettings.create(system, topicConnectionFactory).withTopic(testTopicName));
    // #ibmmq-topic

    CompletionStage<List<String>> result =
        jmsTopicSource.take(in.size()).runWith(Sink.seq(), system);

    Thread.sleep(500);

    Source.from(in).runWith(jmsTopicSink, system);

    assertEquals(in, result.toCompletableFuture().get(5, TimeUnit.SECONDS));
  }

  // #ibmmq-custom-destination
  Function<Session, Destination> createQueue(String destinationName) {
    return (session) -> {
      // cast to correct session implementation: MQQueueSession, MQTopicSession, MQSession
      MQQueueSession mqSession = (MQQueueSession) session;
      try {
        return mqSession.createQueue(destinationName);
      } catch (JMSException e) {
        throw new RuntimeException(e);
      }
    };
  }
  // #ibmmq-custom-destination

}
