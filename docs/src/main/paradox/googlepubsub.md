# Google Pub Sub

The google pub sub connector provides a way to connect to google clouds managed pub sub https://cloud.google.com/pubsub/. A global service for real-time and reliable messaging and streaming data.

## Artifacts

sbt
:   @@@vars
    ```scala
    libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-google-pubsub" % "$version$"
    ```
    @@@

Maven
:   @@@vars
    ```xml
    <dependency>
      <groupId>com.lightbend.akka</groupId>
      <artifactId>akka-stream-alpakka-google-pubsub_$scala.binaryVersion$</artifactId>
      <version>$version$</version>
    </dependency>
    ```
    @@@

Gradle
:   @@@vars
    ```gradle
    dependencies {
      compile group: "com.lightbend.akka", name: "akka-stream-alpakka-google-pubsub_$scala.binaryVersion$", version: "$version$"
    }
    ```
    @@@
                                                                                                                          
## Usage

Prepare your credentials for access to google pub sub.

Scala
: @@snip (../../../../googlepubsub/src/test/scala/akka/stream/alpakka/googlepubsub/ExampleUsage.scala) { #init-credentials }

And prepare the actor system and materializer.

Scala
: @@snip (../../../../googlepubsub/src/test/scala/akka/stream/alpakka/googlepubsub/ExampleUsage.scala) { #init-mat }

To publish a single request, build the message with a base64 data payload and put it in a PublishRequest. Publishing creates a flow taking the messages and returning the accepted message ids.

Scala
: @@snip (../../../../googlepubsub/src/test/scala/akka/stream/alpakka/googlepubsub/ExampleUsage.scala) { #publish-single }

To get greater performance you can batch messages together, here we send batches with a maximum size of 1000 or at a maximum of 1 minute apart depending on the source.

Scala
: @@snip (../../../../googlepubsub/src/test/scala/akka/stream/alpakka/googlepubsub/ExampleUsage.scala) { #publish-fast }

To consume the messages from a subscription you must subscribe then acknowledge the received messages.

Scala
: @@snip (../../../../googlepubsub/src/test/scala/akka/stream/alpakka/googlepubsub/ExampleUsage.scala) { #subscribe }

If you want to automatically acknowledge the messages and send the ReceivedMessages to your own sink you can create a graph.

Scala
: @@snip (../../../../googlepubsub/src/test/scala/akka/stream/alpakka/googlepubsub/ExampleUsage.scala) { #subscribe-auto-ack }