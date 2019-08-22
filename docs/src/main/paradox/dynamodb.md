# AWS DynamoDB

The AWS DynamoDB connector provides a flow for streaming DynamoDB requests. For more information about DynamoDB please visit the [official documentation](https://aws.amazon.com/dynamodb/).

@@project-info{ projectId="dynamodb" }

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-dynamodb_$scala.binary.version$
  version=$project.version$
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="dynamodb" }


## Setup

This connector requires a @javadoc[DynamoDbAsyncClient](software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient) instance to communicate with AWS DynamoDB.

It is your code's responsibility to call `close` to free any resources held by the client. In this example it will be called when the actor system is terminated.

Scala
: @@snip [snip](/dynamodb/src/test/scala/docs/scaladsl/ExampleSpec.scala) { #init-client }

Java
: @@snip [snip](/dynamodb/src/test/java/docs/javadsl/ExampleTest.java) { #init-client }

This connector is set up to use @extref:[Akka HTTP](akka-http:) as default HTTP client via the thin adapter library [AWS Akka-Http SPI implementation](https://github.com/matsluni/aws-spi-akka-http). By setting the `httpClient` explicitly (as above) the Akka actor system is reused, if not set explicitly a separate actor system will be created internally.

It is possible to configure the use of Netty instead, which is Amazon's default. Add an appropriate Netty version to the dependencies and configure @javadoc[`NettyNioAsyncHttpClient`](software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient).


## Sending requests and receiving responses

For simple operations you can issue a single request, and get back the result in a @scala[`Future`]@java[`CompletionStage`].

Scala
: @@snip [snip](/dynamodb/src/test/scala/docs/scaladsl/ExampleSpec.scala) { #simple-request }

Java
: @@snip [snip](/dynamodb/src/test/java/docs/javadsl/ExampleTest.java) { #simple-request }

You can also get the response to a request as an element emitted from a Flow:

Scala
: @@snip [snip](/dynamodb/src/test/scala/docs/scaladsl/ExampleSpec.scala) { #flow }

Java
: @@snip [snip](/dynamodb/src/test/java/docs/javadsl/ExampleTest.java) { #flow }

Some DynamoDB operations, such as ListTables, Query and Scan, are paginated by nature.
This is how you can get a stream of all result pages:

Scala
: @@snip [snip](/dynamodb/src/test/scala/docs/scaladsl/ExampleSpec.scala) { #paginated }

Java
: @@snip [snip](/dynamodb/src/test/java/docs/javadsl/ExampleTest.java) { #paginated }


## Handling failed requests

By default the stream is stopped if a request fails with server error, or the response can not be parsed.

To handle failed requests later in the stream use @scala[@scaladoc[DynamoDb.tryFlow](akka.stream.alpakka.dynamodb.scaladsl.DynamoDb$)]@java[@scaladoc[DynamoDb.tryFlow](akka.stream.alpakka.dynamodb.javadsl.DynamoDb$)]. The responses will be wrapped with a @scaladoc[Try](scala.util.Try).

This flow composes easily with a Akka Stream RetryFlow, that allows to selectively retry requests with a backoff.

For example to retry all of the failed requests, wrap the `DynamoDb.tryFlow` with `RetryFlow.withBackoff` like so:

Scala
: @@snip [snip](/dynamodb/src/test/scala/docs/scaladsl/RetrySpec.scala) { #create-retry-flow }

Java
: @@snip [snip](/dynamodb/src/test/java/docs/javadsl/RetryTest.java) { #create-retry-flow }

And then use the new flow to send the requests through:

Scala
: @@snip [snip](/dynamodb/src/test/scala/docs/scaladsl/RetrySpec.scala) { #use-retry-flow }

Java
: @@snip [snip](/dynamodb/src/test/java/docs/javadsl/RetryTest.java) { #use-retry-flow }


## Running the example code

The code in this guide is part of runnable tests of this project. You are welcome to edit the code and run it in sbt.

> Test code requires DynamoDB running in the background. You can start one quickly using docker:
>
> `docker-compose up dynamodb`

Scala
:   ```
    sbt
    > dynamodb/testOnly *Spec
    ```

Java
:   ```
    sbt
    > dynamodb/testOnly *Test
    ```
