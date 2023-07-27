# Using IBM MQ

You can use IBM MQ like any other JMS Provider by creating a `QueueConnectionFactory` or a `TopicConnectionFactory`
and creating a `JmsConsumerSettings` or `JmsProducerSettings` from it.
The below snippets have been tested with a default IBM MQ docker image which contains queues and topics for testing.
The following command starts MQ 9 using docker:

    docker run --env LICENSE=accept --env MQ_QMGR_NAME=QM1 --publish 1414:1414 --publish 9443:9443 ibmcom/mq:9.1.1.0

MQ settings for this image are shown here: https://github.com/ibm-messaging/mq-docker#mq-developer-defaults

## Artifacts

The Akka dependencies are available from Akka's library repository. To access them there, you need to configure the URL for this repository.

@@repository [Maven,sbt,Gradle] {
id="akka-repository"
name="Akka library repository"
url="https://repo.akka.io/maven"
}

Additionally, add the dependencies as below.

@@dependency [sbt,Maven,Gradle] {
  group1=com.lightbend.akka
  artifact1=akka-stream-alpakka-jms_$scala.binary.version$
  version1=$project.version$
  group2=javax.jms
  artifact2=jms
  version2=1.1
  group3=com.ibm.mq
  artifact3=com.ibm.mq.allclient
  version3=9.1.1.0
}

## Create a MQConnectionFactory
The `MQConnectionFactory` needs a queue manager name and a channel name, the docker command used in the previous section sets up a `QM1` queue manager and a `DEV.APP.SVRCONN` channel. The IBM MQ client makes it possible to
connect to the MQ server over TCP/IP or natively through JNI (when the client and server run on the same machine). In the examples below we have chosen to use TCP/IP, which is done by setting the transport type to `CommonConstants.WMQ_CM_CLIENT`.

Depending on the connection target, choose an appropriate implementation for the connection factory.

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsIbmmqConnectorsSpec.scala) { #ibmmq-connection-factory }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsIbmmqConnectorsTest.java) { #ibmmq-connection-factory} 

## Create a JmsConsumer and JmsProducer to a Queue

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsIbmmqConnectorsSpec.scala) { #ibmmq-queue }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsIbmmqConnectorsTest.java) { #ibmmq-queue} 

## Create a JmsConsumer and JmsProducer to a Topic
The IBM MQ docker container sets up a `dev/` topic, which is used in the example below.

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsIbmmqConnectorsSpec.scala) { #ibmmq-topic }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsIbmmqConnectorsTest.java) { #ibmmq-topic } 

## Create a JmsConsumer and JmsProducer to custom destination
Example with custom queue.

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsIbmmqConnectorsSpec.scala) { #ibmmq-custom-destination }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsIbmmqConnectorsTest.java) { #ibmmq-custom-destination } 
