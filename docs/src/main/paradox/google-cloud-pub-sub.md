# Google Cloud Pub/Sub

The google cloud pub/sub connector provides a way to connect to google clouds managed pub/sub https://cloud.google.com/pubsub/.

We provide two means of communicating to PubSub:
1. Via normal HTTP requests (i.e. https://pubsub.googleapis.com)
2. Via GRPC protocol, using Google's provided Java libraries.


### Reported issues

[Tagged issues at Github](https://github.com/akka/alpakka/labels/p%3Agoogle-cloud-pub-sub)


## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-google-cloud-pub-sub_$scalaBinaryVersion$
  version=$version$
}

## Usage (HTTP)

Prepare your credentials for access to google cloud pub/sub.

Scala
: @@snip ($alpakka$/google-cloud-pub-sub/src/test/scala/akka/stream/alpakka/googlecloud/pubsub/ExampleUsage.scala) { #init-credentials }

Java
: @@snip ($alpakka$/google-cloud-pub-sub/src/test/java/akka/stream/alpakka/googlecloud/pubsub/ExampleUsageJava.java) { #init-credentials }

And prepare the actor system and materializer.

Scala
: @@snip ($alpakka$/google-cloud-pub-sub/src/test/scala/akka/stream/alpakka/googlecloud/pubsub/ExampleUsage.scala) { #init-mat }

Java
: @@snip ($alpakka$/google-cloud-pub-sub/src/test/java/akka/stream/alpakka/googlecloud/pubsub/ExampleUsageJava.java) { #init-mat }

To publish a single request, build the message with a base64 data payload and put it in a @scaladoc[PublishRequest](akka.stream.alpakka.googlecloud.pubsub.PublishRequest). Publishing creates a flow taking the messages and returning the accepted message ids.

Scala
: @@snip ($alpakka$/google-cloud-pub-sub/src/test/scala/akka/stream/alpakka/googlecloud/pubsub/ExampleUsage.scala) { #publish-single }

Java
: @@snip ($alpakka$/google-cloud-pub-sub/src/test/java/akka/stream/alpakka/googlecloud/pubsub/ExampleUsageJava.java) { #publish-single }

To get greater performance you can batch messages together, here we send batches with a maximum size of 1000 or at a maximum of 1 minute apart depending on the source.

Scala
: @@snip ($alpakka$/google-cloud-pub-sub/src/test/scala/akka/stream/alpakka/googlecloud/pubsub/ExampleUsage.scala) { #publish-fast }

Java
: @@snip ($alpakka$/google-cloud-pub-sub/src/test/java/akka/stream/alpakka/googlecloud/pubsub/ExampleUsageJava.java) { #publish-fast }

To consume the messages from a subscription you must subscribe then acknowledge the received messages. @scaladoc[PublishRequest](akka.stream.alpakka.googlecloud.pubsub.ReceivedMessage)

Scala
: @@snip ($alpakka$/google-cloud-pub-sub/src/test/scala/akka/stream/alpakka/googlecloud/pubsub/ExampleUsage.scala) { #subscribe }

Java
: @@snip ($alpakka$/google-cloud-pub-sub/src/test/java/akka/stream/alpakka/googlecloud/pubsub/ExampleUsageJava.java) { #subscribe }

If you want to automatically acknowledge the messages and send the ReceivedMessages to your own sink you can create a graph.

Scala
: @@snip ($alpakka$/google-cloud-pub-sub/src/test/scala/akka/stream/alpakka/googlecloud/pubsub/ExampleUsage.scala) { #subscribe-auto-ack }

Java
: @@snip ($alpakka$/google-cloud-pub-sub/src/test/java/akka/stream/alpakka/googlecloud/pubsub/ExampleUsageJava.java) { #subscribe-auto-ack }

## Usage (GRPC)

Credentials are automatically collected from your `GOOGLE_APPLICATION_CREDENTIAL` environment variable. Please check
[Google official documentation](https://cloud.google.com/pubsub/docs/reference/libraries#setting_up_authentication) for more details.

Prepare the actor system and materializer:

Scala
: @@snip ($alpakka$/google-cloud-pub-sub/src/test/scala/akka/stream/alpakka/googlecloud/pubsub/ExampleUsageGrpc.scala) { #init-mat }

Java
: @@snip ($alpakka$/google-cloud-pub-sub/src/test/java/akka/stream/alpakka/googlecloud/pubsub/ExampleUsageJavaGrpc.java) { #init-mat }

### Client configuration

We need to configure the client which will connect using GRPC.

Scala
: @@snip ($alpakka$/google-cloud-pub-sub/src/test/scala/akka/stream/alpakka/googlecloud/pubsub/ExampleUsageGrpc.scala) { #init-client }

Java
: @@snip ($alpakka$/google-cloud-pub-sub/src/test/java/akka/stream/alpakka/googlecloud/pubsub/ExampleUsageJavaGrpc.java) { #init-client }

The option `usePlaintext` is useful when used in conjunction with [PubSub emulator](https://cloud.google.com/pubsub/docs/emulator).
`returnImmediately` and `maxMessages` match the spec as given in the [Google PubSub documentation](https://cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions/pull).

The parallelism parameter allows more than a single in-flight request. See [GitHub PR #759](https://github.com/akka/alpakka/pull/759) for more details.

### Publishing 

We first construct a message and then a request using Google's builders. We declare a singleton source which will go via our publishing flow. All messages sent to the flow are published to PubSub.

Scala
: @@snip ($alpakka$/google-cloud-pub-sub/src/test/scala/akka/stream/alpakka/googlecloud/pubsub/ExampleUsageGrpc.scala) { #publish-single }

Java
: @@snip ($alpakka$/google-cloud-pub-sub/src/test/java/akka/stream/alpakka/googlecloud/pubsub/ExampleUsageJavaGrpc.java) { #publish-single }


Similarly to before, we can publish a batch of messages for greater efficiency.

Scala
: @@snip ($alpakka$/google-cloud-pub-sub/src/test/scala/akka/stream/alpakka/googlecloud/pubsub/ExampleUsageGrpc.scala) { #publish-fast }

Java
: @@snip ($alpakka$/google-cloud-pub-sub/src/test/java/akka/stream/alpakka/googlecloud/pubsub/ExampleUsageJavaGrpc.java) { #publish-fast }

Finally, to automatically acknowledge messages and send messages to your own sink, once can do the following:

Scala
: @@snip ($alpakka$/google-cloud-pub-sub/src/test/scala/akka/stream/alpakka/googlecloud/pubsub/ExampleUsageGrpc.scala) { #subscribe-auto-ack }

Java
: @@snip ($alpakka$/google-cloud-pub-sub/src/test/java/akka/stream/alpakka/googlecloud/pubsub/ExampleUsageJavaGrpc.java) { #subscribe-auto-ack }

## Running the examples

To run the example code you will need to configure a project and pub/sub in google cloud and provide your own credentials.
