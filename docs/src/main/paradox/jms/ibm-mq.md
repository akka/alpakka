# Using IBM MQ

You can use IBM MQ like any other JMS Provider by creating a `QueueConnectionFactory` or a `TopicConnectionFactory`
and creating a `JmsConsumerSettings` or `JmsProducerSettings` from it.
The below snippets have been tested with a default IBM MQ docker image which contains queues and topics for testing.
The following command starts MQ 9 using docker:

    docker run --env LICENSE=accept --env MQ_QMGR_NAME=QM1 --publish 1414:1414 --publish 9443:9443 ibmcom/mq:9

MQ settings for this image are shown here: https://github.com/ibm-messaging/mq-docker#mq-developer-defaults

## Create a JmsConsumer to an IBM MQ Queue

The `MQQueueConnectionFactory` needs a queue manager name and a channel name, the docker command used in the previous section sets up a `QM1` queue manager and a `DEV.APP.SVRCONN` channel. The IBM MQ client makes it possible to
connect to the MQ server over TCP/IP or natively through JNI (when the client and server run on the same machine). In the examples below we have chosen to use TCP/IP, which is done by setting the transport type to `CommonConstants.WMQ_CM_CLIENT`.

Scala
:   &#9;

    ```scala
    import com.ibm.mq.jms.MQQueueConnectionFactory
    import com.ibm.msg.client.wmq.common.CommonConstants

    val QueueManagerName = "QM1"
    val TestChannelName = "DEV.APP.SVRCONN"

    // Create the IBM MQ QueueConnectionFactory
    val queueConnectionFactory = new MQQueueConnectionFactory()
    queueConnectionFactory.setQueueManager(QueueManagerName)
    queueConnectionFactory.setChannel(TestChannelName)

    // Connect to IBM MQ over TCP/IP
    queueConnectionFactory.setTransportType(CommonConstants.WMQ_CM_CLIENT)
    val TestQueueName = "DEV.QUEUE.1"

    // Option1: create Source using default factory with just name
    val jmsSource: Source[String, NotUsed] = JmsConsumer.textSource(
      JmsConsumerSettings(system, queueConnectionFactory).withQueue(TestQueueName)
    )

    // Option2: create Source using custom factory
    private def createMqQueue(destinationName: String): Session => MQQueue = { session =>
       ...
    }

    val jmsSource: Source[String, NotUsed] = JmsConsumer.textSource(
      JmsConsumerSettings(system, queueConnectionFactory)
        .withDestination(CustomDestination(TestQueueName, createMqQueue(TestQueueName)))
    )
    ```

Java
:   &#9;

    ```java
    import com.ibm.mq.jms.MQQueueConnectionFactory;
    import com.ibm.msg.client.wmq.common.CommonConstants;

    String queueManagerName = "QM1";
    String testChannelName = "DEV.APP.SVRCONN";

    // Create the IBM MQ QueueConnectionFactory
    MQQueueConnectionFactory queueConnectionFactory = new MQQueueConnectionFactory();
    queueConnectionFactory.setQueueManager(queueManagerName);
    queueConnectionFactory.setChannel(testChannelName);

    // Connect to IBM MQ over TCP/IP
    queueConnectionFactory.setTransportType(CommonConstants.WMQ_CM_CLIENT);
    String testQueueName = "DEV.QUEUE.1";

    // Option1: create Source using default factory with just name
    Source<String, NotUsed> jmsSource = JmsConsumer.textSource(
      JmsConsumerSettings
        .create(system, queueConnectionFactory)
        .withQueue(testQueueName)
    );

    // Option2: create Source using custom factory
    private Function1<Session, Destination> createMqQueue(String destinationName) {
        return (session) -> {
            ...
        };
    }

    Source<String, NotUsed> jmsSource = JmsConsumer.textSource(
      JmsConsumerSettings
        .create(system, queueConnectionFactory)
        .withDestination(new CustomDestination(testQueueName,createMqQueue(testQueueName)))
    );
    ```

## Create a JmsProducer to an IBM MQ Topic
The IBM MQ docker container sets up a `dev/` topic, which is used in the example below.

Scala
:   &#9;

    ```scala
    import com.ibm.mq.jms.MQTopicConnectionFactory
    import com.ibm.msg.client.wmq.common.CommonConstants

    val QueueManagerName = "QM1"
    val TestChannelName = "DEV.APP.SVRCONN"

    // Create the IBM MQ TopicConnectionFactory
    val topicConnectionFactory = new MQTopicConnectionFactory()
    topicConnectionFactory.setQueueManager(QueueManagerName)
    topicConnectionFactory.setChannel(TestChannelName)

    // Connect to IBM MQ over TCP/IP
    topicConnectionFactory.setTransportType(CommonConstants.WMQ_CM_CLIENT)
    val TestTopicName = "dev/"

    // Option1: create Sink using default factory with just name
    val jmsTopicSink: Sink[String, NotUsed] = JmsProducer(
      JmsProducerSettings(system, topicConnectionFactory).withTopic(TestTopicName)
    )

    // Option2: create Sink using custom factory
    private def createMqTopic(destinationName: String): Session => MQTopic = { session =>
        ...
    }    

    val jmsTopicSink: Sink[String, NotUsed] = JmsProducer(
      JmsProducerSettings(system, topicConnectionFactory)
        .withDestination(CustomDestination(TestTopicName, createMqTopic(TestTopicName)))
    )    
    ```

Java
:   &#9;

    ```java
    import com.ibm.mq.jms.MQTopicConnectionFactory;
    import com.ibm.msg.client.wmq.common.CommonConstants;

    String queueManagerName = "QM1";
    String testChannelName = "DEV.APP.SVRCONN";

    // Create the IBM MQ TopicConnectionFactory
    val topicConnectionFactory = new MQTopicConnectionFactory();
    topicConnectionFactory.setQueueManager(queueManagerName);
    topicConnectionFactory.setChannel(testChannelName);

    // Connect to IBM MQ over TCP/IP
    topicConnectionFactory.setTransportType(CommonConstants.WMQ_CM_CLIENT);
    String testTopicName = "dev/";

    // Option1: create Source using default factory with just name
    Sink<String, NotUsed> jmsTopicSink = JmsProducer.textSink(
      JmsProducerSettings
        .create(system, topicConnectionFactory)
        .withTopic(testTopicName)
    );

    // Option2: create Source using custom factory
    private Function1<Session, Destination> createMqTopic(String destinationName) {
        return (session) -> {
            ...
        };
    }

    Sink<String, NotUsed> jmsTopicSink = JmsProducer.textSink(
      JmsProducerSettings
        .create(system, queueConnectionFactory)
        .withDestination(new CustomDestination(testTopicName, createMqTopic(testTopicName)))
    );    
    ```
