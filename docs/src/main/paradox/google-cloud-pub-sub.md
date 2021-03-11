# Google Cloud Pub/Sub

@@@ note
Google Cloud Pub/Sub provides many-to-many, asynchronous messaging that decouples senders and receivers.

Further information at the official [Google Cloud documentation website](https://cloud.google.com/pubsub/docs/overview).
@@@

This connector communicates to Pub/Sub via HTTP requests (i.e. `https://pubsub.googleapis.com`). For a connector that uses gRPC for the communication, take a look at the alternative @ref[Alpakka Google Cloud Pub/Sub gRPC](google-cloud-pub-sub-grpc.md) connector.

@@project-info{ projectId="google-cloud-pub-sub" }

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-google-cloud-pub-sub_$scala.binary.version$
  version=$project.version$
  symbol2=AkkaVersion
  value2=$akka.version$
  group2=com.typesafe.akka
  artifact2=akka-stream_$scala.binary.version$
  version2=AkkaVersion
  symbol3=AkkaHttpVersion
  value3=$akka-http.version$
  group3=com.typesafe.akka
  artifact3=akka-http_$scala.binary.version$
  version3=AkkaHttpVersion
  group4=com.typesafe.akka
  artifact4=akka-http-spray-json_$scala.binary.version$
  version4=AkkaHttpVersion
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="google-cloud-pub-sub" }


## Usage

Prepare your credentials for access to google cloud pub/sub.

Scala
: @@snip [snip](/google-cloud-pub-sub/src/test/scala/docs/scaladsl/ExampleUsage.scala) { #init-credentials }

Java
: @@snip [snip](/google-cloud-pub-sub/src/test/java/docs/javadsl/ExampleUsageJava.java) { #init-credentials }

And prepare the actor system.

Scala
: @@snip [snip](/google-cloud-pub-sub/src/test/scala/docs/scaladsl/ExampleUsage.scala) { #init-system }

Java
: @@snip [snip](/google-cloud-pub-sub/src/test/java/docs/javadsl/ExampleUsageJava.java) { #init-system }

To publish a single request, build the message with a base64 data payload and put it in a @scaladoc[PublishRequest](akka.stream.alpakka.googlecloud.pubsub.PublishRequest). Publishing creates a flow taking the messages and returning the accepted message ids.

Scala
: @@snip [snip](/google-cloud-pub-sub/src/test/scala/docs/scaladsl/ExampleUsage.scala) { #publish-single }

Java
: @@snip [snip](/google-cloud-pub-sub/src/test/java/docs/javadsl/ExampleUsageJava.java) { #publish-single }

To get greater performance you can batch messages together, here we send batches with a maximum size of 1000 or at a maximum of 1 minute apart depending on the source.

Scala
: @@snip [snip](/google-cloud-pub-sub/src/test/scala/docs/scaladsl/ExampleUsage.scala) { #publish-fast }

Java
: @@snip [snip](/google-cloud-pub-sub/src/test/java/docs/javadsl/ExampleUsageJava.java) { #publish-fast }

To consume the messages from a subscription you must subscribe then acknowledge the received messages. @scaladoc[PublishRequest](akka.stream.alpakka.googlecloud.pubsub.ReceivedMessage)

Scala
: @@snip [snip](/google-cloud-pub-sub/src/test/scala/docs/scaladsl/ExampleUsage.scala) { #subscribe }

Java
: @@snip [snip](/google-cloud-pub-sub/src/test/java/docs/javadsl/ExampleUsageJava.java) { #subscribe }

If you want to automatically acknowledge the messages and send the ReceivedMessages to your own sink you can create a graph.

Scala
: @@snip [snip](/google-cloud-pub-sub/src/test/scala/docs/scaladsl/ExampleUsage.scala) { #subscribe-auto-ack }

Java
: @@snip [snip](/google-cloud-pub-sub/src/test/java/docs/javadsl/ExampleUsageJava.java) { #subscribe-auto-ack }

## Running the examples

To run the example code you will need to configure a project and pub/sub in google cloud and provide your own credentials.
