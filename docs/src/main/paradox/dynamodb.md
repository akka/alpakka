# AWS DynamoDB Connector

The AWS DynamoDB connector provides a flow for streaming DynamoDB requests. For more information about DynamoDB please visit the [official documentation](https://aws.amazon.com/dynamodb/).

## Artifacts

sbt
:   @@@vars
    ```scala
    libraryDependencies += "com.typesafe.akka" %% "akka-stream-alpakka-dynamodb" % "$version$"
    ```
    @@@

Maven
:   @@@vars
    ```xml
    <dependency>
      <groupId>com.typesafe.akka</groupId>
      <artifactId>akka-stream-alpakka-dynamodb_$scala.binaryVersion$</artifactId>
      <version>$version$</version>
    </dependency>
    ```
    @@@

Gradle
:   @@@vars
    ```gradle
    dependencies {
      compile group: "com.typesafe.akka", name: "akka-stream-alpakka-dynamodb_$scala.binaryVersion$", version: "$version$"
    }
    ```
    @@@

## Usage

This connector uses the [default credential provider chain](http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html) provided by the [DynamoDB Java SDK](http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/basics.html) to retrieve credentials.

Before you can construct the client, you need an @scaladoc[ActorSystem](akka.actor.ActorSystem), @scaladoc[ActorMaterializer](akka.stream.ActorMaterializer), and @scaladoc[ExecutionContext](scala.concurrent.ExecutionContext).

Scala
: @@snip (../../../../dynamodb/src/test/scala/akka/stream/alpakka/dynamodb/ExampleSpec.scala) { #init-client }

Java
: @@snip (../../../../dynamodb/src/test/java/akka/stream/alpakka/dynamodb/ExampleJavaSpec.java) { #init-client }

You can then create the client with a settings object.

Scala
: @@snip (../../../../dynamodb/src/test/scala/akka/stream/alpakka/dynamodb/ExampleSpec.scala) { #client-construct }

Java
: @@snip (../../../../dynamodb/src/test/java/akka/stream/alpakka/dynamodb/ExampleJavaSpec.java) { #client-construct }

We can now send requests to DynamoDB across the connection.

Scala
: @@snip (../../../../dynamodb/src/test/scala/akka/stream/alpakka/dynamodb/ExampleSpec.scala) { #simple-request }

Java
: @@snip (../../../../dynamodb/src/test/java/akka/stream/alpakka/dynamodb/ExampleJavaSpec.java) { #simple-request }
