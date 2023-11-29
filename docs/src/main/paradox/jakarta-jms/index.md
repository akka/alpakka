# Jakarta Messaging (JMS)

@@@ note { title="Jakarta Messaging (JMS)" }

The Jakarta Messaging API (formerly Java Message Service or JMS API) is a Java application programming interface (API) for message-oriented middleware. It provides generic messaging models, able to handle the producer–consumer problem, that can be used to facilitate the sending and receiving of messages between software systems. Jakarta Messaging is a part of [Jakarta EE]((https://jakarta.ee)) and was originally defined by a specification developed at Sun Microsystems before being guided by the Java Community Process.

-- [Wikipedia](https://en.wikipedia.org/wiki/Jakarta_Messaging)

@@@

The Alpakka Jakarta Messaging connector provides Akka Stream sources and sinks to connect to Jakarta Messaging providers.

@@project-info{ projectId="jakarta-jms" }

## Artifacts

The Akka dependencies are available from Akka's library repository. To access them there, you need to configure the URL for this repository.

@@repository [sbt,Maven,Gradle] {
id="akka-repository"
name="Akka library repository"
url="https://repo.akka.io/maven"
}

Additionally, add the dependencies as below.

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-jakarta-jms_$scala.binary.version$
  version=$project.version$
}

@@toc { depth=2 }

@@@ index

* [p](producer.md)
* [c](consumer.md)
* [c](browse.md)

@@@
