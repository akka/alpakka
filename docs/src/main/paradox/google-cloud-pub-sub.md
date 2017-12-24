# Google Cloud Pub/Sub

The google cloud pub/sub connector provides a way to connect to google clouds managed pub/sub https://cloud.google.com/pubsub/.

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-google-cloud-pub-sub_$scalaBinaryVersion$
  version=$version$
}

## Usage

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

## Running the examples

To run the example code you will need to configure a project and pub/sub in google cloud and provide your own credentials.
