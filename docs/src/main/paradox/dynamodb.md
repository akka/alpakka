# AWS DynamoDB

The AWS DynamoDB connector provides a flow for streaming DynamoDB requests. For more information about DynamoDB please visit the [official documentation](https://aws.amazon.com/dynamodb/).

@@project-info{ projectId="dynamodb" }

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
  artifact=akka-stream-alpakka-dynamodb_$scala.binary.version$
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

The example above uses @extref:[Akka HTTP](akka-http:) as the default HTTP client implementation. For more details about the HTTP client, configuring request retrying and best practices for credentials, see @ref[AWS client configuration](aws-shared-configuration.md) for more details.


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


### Flow with context

The `flowWithContext` allows to send an arbitrary value, such as commit handles for JMS or Kafka, past the DynamoDb operation.
The responses are wrapped in a @scaladoc[Try](scala.util.Try) to differentiate between successful operations and errors in-stream.

Scala
: @@snip [snip](/dynamodb/src/test/scala/docs/scaladsl/ExampleSpec.scala) { #withContext }

Java
: @@snip [snip](/dynamodb/src/test/java/docs/javadsl/ExampleTest.java) { #withContext }


### Pagination

The DynamoDB operations `BatchGetItem`, `ListTables`, `Query` and `Scan` allow paginating of results.
The requests with paginated results can be used as source or in a flow with `flowPaginated`:

Scala
: @@snip [snip](/dynamodb/src/test/scala/docs/scaladsl/ExampleSpec.scala) { #paginated }

Java
: @@snip [snip](/dynamodb/src/test/java/docs/javadsl/ExampleTest.java) { #paginated }


## Error Retries and Exponential Backoff

The AWS SDK 2 implements error retrying with exponential backoff which is configurable via the @javadoc[DynamoDbAsyncClient](software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient) configuration by using the @javadoc[RetryPolicy](software.amazon.awssdk.core.retry.RetryPolicy) in `overrideConfiguration`.

See @ref[AWS Retry configuration](aws-shared-configuration.md) for more details.

Scala
: @@snip [snip](/dynamodb/src/test/scala/docs/scaladsl/RetrySpec.scala) { #clientRetryConfig }

Java
: @@snip [snip](/dynamodb/src/test/java/docs/javadsl/RetryTest.java) { #clientRetryConfig }

@@@ index

* [retry conf](aws-shared-configuration.md)

@@@
