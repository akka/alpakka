# AWS DynamoDB Connector

The AWS DynamoDB connector provides a flow for streaming DynamoDB requests. For more information about DynamoDB please visit the [official documentation](https://aws.amazon.com/dynamodb/).

## Artifacts

sbt
:   @@@vars
    ```scala
    libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-dynamodb" % "$version$"
    ```
    @@@

Maven
:   @@@vars
    ```xml
    <dependency>
      <groupId>com.lightbend.akka</groupId>
      <artifactId>akka-stream-alpakka-dynamodb_$scalaBinaryVersion$</artifactId>
      <version>$version$</version>
    </dependency>
    ```
    @@@

Gradle
:   @@@vars
    ```gradle
    dependencies {
      compile group: "com.lightbend.akka", name: "akka-stream-alpakka-dynamodb_$scalaBinaryVersion$", version: "$version$"
    }
    ```
    @@@

## Usage

This connector uses the [default credential provider chain](http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html) provided by the [DynamoDB Java SDK](http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/basics.html) to retrieve credentials.

Before you can construct the client, you need an @scaladoc[ActorSystem](akka.actor.ActorSystem), @scaladoc[ActorMaterializer](akka.stream.ActorMaterializer), and @scaladoc[ExecutionContext](scala.concurrent.ExecutionContext).

Scala
: @@snip (../../../../dynamodb/src/test/scala/akka/stream/alpakka/dynamodb/ExampleSpec.scala) { #init-client }

Java
: @@snip (../../../../dynamodb/src/test/java/akka/stream/alpakka/dynamodb/ExampleTest.java) { #init-client }

You can then create the client with a settings object.

Scala
: @@snip (../../../../dynamodb/src/test/scala/akka/stream/alpakka/dynamodb/ExampleSpec.scala) { #client-construct }

Java
: @@snip (../../../../dynamodb/src/test/java/akka/stream/alpakka/dynamodb/ExampleTest.java) { #client-construct }

We can now send requests to DynamoDB across the connection.

Scala
: @@snip (../../../../dynamodb/src/test/scala/akka/stream/alpakka/dynamodb/ExampleSpec.scala) { #simple-request }

Java
: @@snip (../../../../dynamodb/src/test/java/akka/stream/alpakka/dynamodb/ExampleTest.java) { #simple-request }

### Running the example code

The code in this guide is part of runnable tests of this project. You are welcome to edit the code and run it in sbt.

> Test code requires DynamoDB server running in the background. You can start one quickly using docker:
>
> `docker run --rm -p 8001:8000 deangiberson/aws-dynamodb-local`

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
