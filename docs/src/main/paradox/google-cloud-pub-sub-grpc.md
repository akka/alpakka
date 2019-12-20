# Google Cloud Pub/Sub gRPC

@@@ note
Google Cloud Pub/Sub provides many-to-many, asynchronous messaging that decouples senders and receivers.

Further information at the official [Google Cloud documentation website](https://cloud.google.com/pubsub/docs/overview).
@@@

This connector communicates to Pub/Sub via the gRPC protocol. The integration between Akka Stream and gRPC is handled by the
[Akka gRPC library](https://github.com/akka/akka-grpc). For a connector that uses HTTP for the communication, take a
look at the alternative @ref[Alpakka Google Cloud Pub/Sub](google-cloud-pub-sub.md) connector.

@@project-info{ projectId="google-cloud-pub-sub-grpc" }

## Artifacts

Akka gRPC uses Akka Discovery internally. Make sure to add Akka Discovery with the same Akka version that the application uses.

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-google-cloud-pub-sub-grpc_$scala.binary.version$
  version=$project.version$
  group2=com.typesafe.akka
  artifact2=akka-discovery_$scala.binary.version$
  version2=Your-Akka-version
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="google-cloud-pub-sub-grpc" }

## Build setup

The Alpakka Google Cloud Pub/Sub gRPC library contains the classes generated from [Google's protobuf specification](https://github.com/googleapis/java-pubsub/tree/master/proto-google-cloud-pubsub-v1/).

@@@note { title="ALPN on JDK 8" }

For use on JDK 8 the ALPN Java agent needs to be set up explicitly.

@@@

### Maven

When using JDK 8: configure your project to use the Java agent for ALPN and add `-javaagent:...` to your startup scripts as described in the @extref:[Akka gRPC documentation](akka-grpc:/buildtools/maven.html#starting-your-akka-grpc-server-from-maven).

### sbt

When using JDK 8: Configure your project to use the Java agent for ALPN and add `-javaagent:...` to your startup scripts.

Pull in the [`sbt-javaagent`](https://github.com/sbt/sbt-javaagent) plugin.

project/plugins.sbt
: @@snip (/project/plugins.sbt) { #grpc-agent }

Enable the Akka gRPC and JavaAgent plugins on the sbt project.

build.sbt
: @@snip (/build.sbt) { #grpc-plugins }

Add the Java agent to the runtime configuration.

build.sbt
:   ```scala
    javaAgents += "org.mortbay.jetty.alpn" % "jetty-alpn-agent" % "2.0.9"
    ```

### Gradle

When using JDK 8: Configure your project to use the Java agent for ALPN and add `-javaagent:...` to your startup scripts as described in the @extref:[Akka gRPC documentation](akka-grpc:/buildtools/gradle.html#starting-your-akka-grpc-server-from-gradle).

## Configuration

The connector comes with the default settings configured to work with the Google Pub Sub endpoint and uses the default way of
locating credentials by looking at the `GOOGLE_APPLICATION_CREDENTIALS` environment variable. Please check
[Google official documentation](https://cloud.google.com/pubsub/docs/reference/libraries#setting_up_authentication) for more details
on how to obtain credentials for your application.

The defaults can be changed (for example when testing against the emulator) by tweaking the reference configuration:

reference.conf
: @@snip (/google-cloud-pub-sub-grpc/src/main/resources/reference.conf)

Test Configuration
: @@snip (/google-cloud-pub-sub-grpc/src/test/resources/application.conf)

A manually initialized @scala[@scaladoc[GrpcPublisher](akka.stream.alpakka.googlecloud.pubsub.grpc.scaladsl.GrpcPublisher)]@java[@scaladoc[GrpcPublisher](akka.stream.alpakka.googlecloud.pubsub.grpc.javadsl.GrpcPublisher)] or @scala[@scaladoc[GrpcSubscriber](akka.stream.alpakka.googlecloud.pubsub.grpc.scaladsl.GrpcSubscriber)]@java[@scaladoc[GrpcSubscriber](akka.stream.alpakka.googlecloud.pubsub.grpc.javadsl.GrpcSubscriber)] can be used by providing it as an attribute to the stream:

Scala
: @@snip (/google-cloud-pub-sub-grpc/src/test/scala/docs/scaladsl/IntegrationSpec.scala) { #attributes }

Java
: @@snip (/google-cloud-pub-sub-grpc/src/test/java/docs/javadsl/IntegrationTest.java) { #attributes }

## Publishing 

We first construct a message and then a request using Google's builders. We declare a singleton source which will go via our publishing flow. All messages sent to the flow are published to PubSub.

Scala
: @@snip (/google-cloud-pub-sub-grpc/src/test/scala/docs/scaladsl/IntegrationSpec.scala) { #publish-single }

Java
: @@snip (/google-cloud-pub-sub-grpc/src/test/java/docs/javadsl/IntegrationTest.java) { #publish-single }


Similarly to before, we can publish a batch of messages for greater efficiency.

Scala
: @@snip (/google-cloud-pub-sub-grpc/src/test/scala/docs/scaladsl/IntegrationSpec.scala) { #publish-fast }

Java
: @@snip (/google-cloud-pub-sub-grpc/src/test/java/docs/javadsl/IntegrationTest.java) { #publish-fast }

## Subscribing

To receive message from a subscription, first we create a `StreamingPullRequest` with a FQRS of a subscription and
a deadline for acknowledgements in seconds. Google requires that only the first `StreamingPullRequest` has the subscription
and the deadline set. This connector takes care of that and clears up the subscription FQRS and the deadline for subsequent
`StreamingPullRequest` messages.

Scala
: @@snip (/google-cloud-pub-sub-grpc/src/test/scala/docs/scaladsl/IntegrationSpec.scala) { #subscribe }

Java
: @@snip (/google-cloud-pub-sub-grpc/src/test/java/docs/javadsl/IntegrationTest.java) { #subscribe }

Here `pollInterval` is the time between `StreamingPullRequest`s are sent when there are no messages in the subscription.

Messages received from the subscription need to be acknowledged or they will be sent again. To do that create
`AcknowledgeRequest` that contains `ackId`s of the messages to be acknowledged and send them to a sink
created by `GooglePubSub.acknowledge`.

Scala
: @@snip (/google-cloud-pub-sub-grpc/src/test/scala/docs/scaladsl/IntegrationSpec.scala) { #acknowledge }

Java
: @@snip (/google-cloud-pub-sub-grpc/src/test/java/docs/javadsl/IntegrationTest.java) { #acknowledge }


## Running the test code

@@@ note
Integration test code requires Google Cloud Pub/Sub emulator running in the background. You can start it quickly using docker:

`docker-compose up -d gcloud-pubsub-client`

This will also run the Pub/Sub admin client that will create topics and subscriptions used by the
integration tests.
@@@

Tests can be started from sbt by running:

sbt
:   ```bash
    > google-cloud-pub-sub-grpc/test
    ```

There is also an @github[ExampleApp](/google-cloud-pub-sub-grpc/src/test/scala/docs/scaladsl/ExampleApp.scala) that can be used
to test publishing to topics and receiving messages from subscriptions.

To run the example app you will need to configure a project and Pub/Sub in Google Cloud and provide your own credentials.

sbt
:   &#9;

    ```bash
    env GOOGLE_APPLICATION_CREDENTIALS=/path/to/application/credentials.json sbt

    // receive messages from a subsciptions
    > google-cloud-pub-sub-grpc/Test/run subscribe <project-id> <subscription-name>

    // publish a single message to a topic
    > google-cloud-pub-sub-grpc/Test/run publish-single <project-id> <topic-name>

    // continually publish a message stream to a topic
    > google-cloud-pub-sub-grpc/Test/run publish-stream <project-id> <topic-name>
    ```
