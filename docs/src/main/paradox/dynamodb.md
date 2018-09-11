# AWS DynamoDB

The AWS DynamoDB connector provides a flow for streaming DynamoDB requests. For more information about DynamoDB please visit the [official documentation](https://aws.amazon.com/dynamodb/).

### Reported issues

[Tagged issues at Github](https://github.com/akka/alpakka/labels/p%3Adynamodb)

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-dynamodb_$scalaBinaryVersion$
  version=$version$
}

## Setup

This connector will uses the [default credential provider chain](http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html) 
provided by the [DynamoDB Java SDK](http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/basics.html) to retrieve credentials.

If you wish to use static credentials they can be defined in the config

Define Static Credentials in `application.conf`
: @@snip [snip](/dynamodb/src/test/scala/akka/stream/alpakka/dynamodb/DynamoSettingsSpec.scala) { #static-creds }

You may attach any type of `AWSCredentialsProvider` programmatically via the `withCredentialsProvider` method in @scaladoc[DynamoSettings](akka.stream.alpakka.dynamodb.DynamoSettings$).

Scala
: @@snip [snip](/dynamodb/src/test/scala/akka/stream/alpakka/dynamodb/DynamoSettingsSpec.scala) { #credentials-provider } 

Java
: @@snip [snip](/dynamodb/src/test/java/docs/javadsl/ExampleTest.java) { #credentials-provider } 


Before you can construct the client, you need an @scaladoc[ActorSystem](akka.actor.ActorSystem), and @scaladoc[ActorMaterializer](akka.stream.ActorMaterializer).

You can then create the client with a settings object.

Scala
: @@snip [snip](/dynamodb/src/test/scala/docs/scaladsl/ExampleSpec.scala) { #client-construct }

Java
: @@snip [snip](/dynamodb/src/test/java/docs/javadsl/ExampleTest.java) { #client-construct }


## Usage

We can now send requests to DynamoDB across the connection.

Scala
: @@snip [snip](/dynamodb/src/test/scala/docs/scaladsl/ExampleSpec.scala) { #simple-request }

Java
: @@snip [snip](/dynamodb/src/test/java/docs/javadsl/ExampleTest.java) { #simple-request }

You can also use a Flow to execute your DynamoDB call:

Scala
: @@snip [snip](/dynamodb/src/test/scala/docs/scaladsl/ExampleSpec.scala) { #flow }

Java
: @@snip [snip](/dynamodb/src/test/java/docs/javadsl/ExampleTest.java) { #flow }

Some DynamoDB operations, such as Query and Scan, are paginated by nature.
You can get a stream of all result pages:

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
