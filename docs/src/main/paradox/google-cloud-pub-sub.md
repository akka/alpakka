# Google Cloud Pub/Sub

The google cloud pub/sub connector provides a way to connect to google clouds managed pub/sub https://cloud.google.com/pubsub/.

## Artifacts

sbt
:   @@@vars
    ```scala
    libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-google-cloud-pub-sub" % "$version$"
    ```
    @@@

Maven
:   @@@vars
    ```xml
    <dependency>
      <groupId>com.lightbend.akka</groupId>
      <artifactId>akka-stream-alpakka-google-cloud-pub-sub_$scalaBinaryVersion$</artifactId>
      <version>$version$</version>
    </dependency>
    ```
    @@@

Gradle
:   @@@vars
    ```gradle
    dependencies {
      compile group: "com.lightbend.akka", name: "akka-stream-alpakka-google-cloud-pub-sub_$scalaBinaryVersion$", version: "$version$"
    }
    ```
    @@@
                                                                                                                          
## Usage

Prepare your credentials for access to google cloud pub/sub.

Scala
: @@snip (../../../../google-cloud-pub-sub/src/test/scala/akka/stream/alpakka/googlecloud/pubsub/ExampleUsage.scala) { #init-credentials }

Java
: @@snip (../../../../google-cloud-pub-sub/src/test/java/akka/stream/alpakka/googlecloud/pubsub/ExampleUsageJava.java) { #init-credentials }

And prepare the actor system and materializer.

Scala
: @@snip (../../../../google-cloud-pub-sub/src/test/scala/akka/stream/alpakka/googlecloud/pubsub/ExampleUsage.scala) { #init-mat }

Java
: @@snip (../../../../google-cloud-pub-sub/src/test/java/akka/stream/alpakka/googlecloud/pubsub/ExampleUsageJava.java) { #init-mat }

To publish a single request, build the message with a base64 data payload and put it in a @scaladoc[PublishRequest](akka.stream.alpakka.googlecloud.pubsub.PublishRequest). Publishing creates a flow taking the messages and returning the accepted message ids.

Scala
: @@snip (../../../../google-cloud-pub-sub/src/test/scala/akka/stream/alpakka/googlecloud/pubsub/ExampleUsage.scala) { #publish-single }

Java
: @@snip (../../../../google-cloud-pub-sub/src/test/java/akka/stream/alpakka/googlecloud/pubsub/ExampleUsageJava.java) { #publish-single }

To get greater performance you can batch messages together, here we send batches with a maximum size of 1000 or at a maximum of 1 minute apart depending on the source.

Scala
: @@snip (../../../../google-cloud-pub-sub/src/test/scala/akka/stream/alpakka/googlecloud/pubsub/ExampleUsage.scala) { #publish-fast }

Java
: @@snip (../../../../google-cloud-pub-sub/src/test/java/akka/stream/alpakka/googlecloud/pubsub/ExampleUsageJava.java) { #publish-fast }

To consume the messages from a subscription you must subscribe then acknowledge the received messages. @scaladoc[PublishRequest](akka.stream.alpakka.googlecloud.pubsub.ReceivedMessage)

Scala
: @@snip (../../../../google-cloud-pub-sub/src/test/scala/akka/stream/alpakka/googlecloud/pubsub/ExampleUsage.scala) { #subscribe }

Java
: @@snip (../../../../google-cloud-pub-sub/src/test/java/akka/stream/alpakka/googlecloud/pubsub/ExampleUsageJava.java) { #subscribe }

If you want to automatically acknowledge the messages and send the ReceivedMessages to your own sink you can create a graph.

Scala
: @@snip (../../../../google-cloud-pub-sub/src/test/scala/akka/stream/alpakka/googlecloud/pubsub/ExampleUsage.scala) { #subscribe-auto-ack }

Java
: @@snip (../../../../google-cloud-pub-sub/src/test/java/akka/stream/alpakka/googlecloud/pubsub/ExampleUsageJava.java) { #subscribe-auto-ack }

## Running the examples

To run the example code you will need to configure a project and pub/sub in google cloud and provide your own credentials.