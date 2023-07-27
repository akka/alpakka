# JMS

@@@ note { title="Java Message Service (JMS)" }

The Java Message Service (JMS) API is a Java message-oriented middleware API for sending messages between two or more clients. It is an implementation to handle the producer–consumer problem. JMS is a part of the [Java Platform, Enterprise Edition (Java EE)](https://docs.oracle.com/javaee/7/tutorial/jms-concepts001.htm#BNCDR), and was defined by a specification developed at Sun Microsystems, but which has since been guided by the Java Community Process. It is a messaging standard that allows application components based on Java EE to create, send, receive, and read messages. It allows the communication between different components of a distributed application to be loosely coupled, reliable, and asynchronous.

-- [Wikipedia](https://en.wikipedia.org/wiki/Java_Message_Service)

@@@

The Alpakka JMS connector provides Akka Stream sources and sinks to connect to JMS providers.

@@project-info{ projectId="jms" }

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
}

@@toc { depth=2 }

@@@ index

* [p](producer.md)
* [c](consumer.md)
* [c](browse.md)
* [ibm](ibm-mq.md)

@@@
