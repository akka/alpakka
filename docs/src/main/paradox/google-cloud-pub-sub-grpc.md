# Google Cloud Pub/Sub gRPC

@@@ note
Google Cloud Pub/Sub provides many-to-many, asynchronous messaging that decouples senders and receivers.

Further information at the official [Google Cloud documentation website](https://cloud.google.com/pubsub/docs/overview).
@@@

This connector communicates to Pub/Sub via the gRPC protocol. The integration between Akka Stream and gRPC is handled by
@extref[Akka gRPC $akka-grpc.version$](akka-grpc:). For a connector that uses HTTP for the communication, take a
look at the alternative @ref[Alpakka Google Cloud Pub/Sub](google-cloud-pub-sub.md) connector.

@@project-info{ projectId="google-cloud-pub-sub-grpc" }

## Artifacts

Akka gRPC uses Akka Discovery internally. Make sure to add Akka Discovery with the same Akka version that the application uses.

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-google-cloud-pub-sub-grpc_$scala.binary.version$
  version=$project.version$
  symbol2=AkkaVersion
  value2=$akka.version$
  group2=com.typesafe.akka
  artifact2=akka-stream_$scala.binary.version$
  version2=AkkaVersion
  group3=com.typesafe.akka
  artifact3=akka-discovery_$scala.binary.version$
  version3=AkkaVersion
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="google-cloud-pub-sub-grpc" }

## Binary compatibility

@@@warning

This connector contains code generated from Protobuf files which is bound to @extref:[Akka gRPC $akka-grpc.version$](akka-grpc:). This makes it @extref:[NOT binary-compatible](akka-grpc:/binary-compatibility.html) with later versions of Akka gRPC.
You can not use a different version of Akka gRPC within the same JVM instance.

@@@

## Build setup

The Alpakka Google Cloud Pub/Sub gRPC library contains the classes generated from [Google's protobuf specification](https://github.com/googleapis/java-pubsub/tree/master/proto-google-cloud-pubsub-v1/).

@@@note { title="ALPN on JDK 8" }

HTTP/2 requires ALPN negotiation, which comes with the JDK starting with
version 8u251.

For older versions of the JDK you will need to load the `jetty-alpn-agent`
yourself, but we recommend upgrading.

@@@

## Configuration

The PubSub gRPC connector @ref[shares its basic configuration](google-common.md) with all the Google connectors in Alpakka.
Additional PubSub-specific configuration settings can be found in its own @github[reference.conf](/google-cloud-pub-sub-grpc/src/main/resources/reference.conf).

The defaults can be changed (for example when testing against the emulator) by tweaking the reference configuration:

reference.conf
: @@snip (/google-cloud-pub-sub-grpc/src/main/resources/reference.conf)

Test Configuration
: @@snip (/google-cloud-pub-sub-grpc/src/test/resources/application.conf)

For more configuration details consider the underlying configuration for @extref:[Akka gRPC](akka-grpc:/client/configuration.html).

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

To receive messages from a subscription, there are two options: `StreamingPullRequest`s or synchronous `PullRequest`s.
To decide whether you should use `StreamingPullRequest` or `PullRequest`, see [StreamingPull: Dealing with large backlogs of small messages](https://cloud.google.com/pubsub/docs/pull#streamingpull_dealing_with_large_backlogs_of_small_messages) and [Synchronous Pull](https://cloud.google.com/pubsub/docs/pull#synchronous_pull) from Google Cloud PubSub's documentation

### StreamingPullRequest
To receive message from a subscription, first we create a `StreamingPullRequest` with a FQRS of a subscription and
a deadline for acknowledgements in seconds. Google requires that only the first `StreamingPullRequest` has the subscription
and the deadline set. This connector takes care of that and clears up the subscription FQRS and the deadline for subsequent
`StreamingPullRequest` messages.

Scala
: @@snip (/google-cloud-pub-sub-grpc/src/test/scala/docs/scaladsl/IntegrationSpec.scala) { #subscribe-stream }

Java
: @@snip (/google-cloud-pub-sub-grpc/src/test/java/docs/javadsl/IntegrationTest.java) { #subscribe-stream }

Here `pollInterval` is the time between `StreamingPullRequest`s are sent when there are no messages in the subscription.

### PullRequest

With `PullRequest`, each request receives a batch of messages, up to a maximum specified by the `maxMessages`.

Scala
: @@snip (/google-cloud-pub-sub-grpc/src/test/scala/docs/scaladsl/IntegrationSpec.scala) { #subscribe-sync }

Java
: @@snip (/google-cloud-pub-sub-grpc/src/test/java/docs/javadsl/IntegrationTest.java) { #subscribe-sync }

Here `pollInterval` is the time between `PullRequest` messages.

In order to minimise latency between requests you can set a buffer on the source. The buffer size depends on the usual
number of messages you receive per each request, if you usually receive the maximum number of messages, it's a good idea
to set the buffer size to be the same as the `maxMessages` parameter. Please note that by having a buffer, you are allowing
messages to spend some of their lease time in the buffer, hence reducing the time to process them before the acknowledgement
deadline is reached. This will depend on your acknowledgement deadline and processing time.

### Acknowledge

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
