# AWS DynamoDB

The AWS DynamoDB connector provides a flow for streaming DynamoDB requests. For more information about DynamoDB please visit the [official documentation](https://aws.amazon.com/dynamodb/).

### Reported issues

[Tagged issues at Github](https://github.com/akka/alpakka/labels/p%3Adynamodb)

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-dynamodb_$scala.binary.version$
  version=$project.version$
}

## Setup

This connector needs a @scaladoc[DynamoClient](akka.stream.alpakka.dynamodb.DynamoClient) instance in order to create stream operators. You can create the client yourself, or let the Akka Extension handle the lifecycle of the client for you.

Factories provided in the @scaladoc[DynamoDb](akka.stream.alpakka.dynamodb.scaladsl.DynamoDb$) will use the client managed by the extension. The managed client will be created using the configuration resolved from `reference.conf` and with overrides from `application.conf`.

Example `application.conf`
: @@snip [snip](/dynamodb/src/test/scala/akka/stream/alpakka/dynamodb/DynamoSettingsSpec.scala) { #static-creds }

`reference.conf`
: @@snip [snip](/dynamodb/src/main/resources/reference.conf)

If the credentials are not set in the configuration, connector will use the [default credential provider chain](http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html) provided by the [DynamoDB Java SDK](http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/basics.html) to retrieve credentials.

There is another set of factories under the @scaladoc[DynamoDbExternal](akka.stream.alpakka.dynamodb.scaladsl.DynamoDbExternal$) which take an instance of @scaladoc[DynamoClient](akka.stream.alpakka.dynamodb.DynamoClient) as a parameter. You might want to use a manually initiated client, if, for example, you need to use a custom credentials provider, which you may attach programmatically via the `withCredentialsProvider` method in @scaladoc[DynamoSettings](akka.stream.alpakka.dynamodb.DynamoSettings$).

Scala
: @@snip [snip](/dynamodb/src/test/scala/akka/stream/alpakka/dynamodb/DynamoSettingsSpec.scala) { #credentials-provider } 

Java
: @@snip [snip](/dynamodb/src/test/java/docs/javadsl/ExampleTest.java) { #credentials-provider }

## Usage

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

Some DynamoDB operations, such as Query and Scan, are paginated by nature.
This is how you can get a stream of all result pages:

Scala
: @@snip [snip](/dynamodb/src/test/scala/docs/scaladsl/ExampleSpec.scala) { #paginated }

Java
: @@snip [snip](/dynamodb/src/test/java/docs/javadsl/ExampleTest.java) { #paginated }

### Running the example code

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
