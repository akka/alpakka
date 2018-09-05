# AWS Lambda

The AWS Lambda Connector provides Akka Flow for AWS Lambda integration.

For more information about AWS Lambda please visit the [AWS lambda documentation](https://aws.amazon.com/documentation/lambda/).

### Reported issues

[Tagged issues at Github](https://github.com/akka/alpakka/labels/p%3Aaws-lambda)


## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-awslambda_$scalaBinaryVersion$
  version=$version$
}

## Usage

Flow provided by this connector need a prepared `AWSLambdaAsyncClient` to be able to invoke lambda functions.

Scala
: @@snip (/awslambda/src/test/scala/docs/scaladsl/Examples.scala) { #init-client }

Java
: @@snip (/awslambda/src/test/java/docs/javadsl/Examples.java) { #init-client }

We will also need an @scaladoc[ActorSystem](akka.actor.ActorSystem) and an @scaladoc[ActorMaterializer](akka.stream.ActorMaterializer).

Scala
: @@snip (/awslambda/src/test/scala/docs/scaladsl/Examples.scala) { #init-mat }

Java
: @@snip (/awslambda/src/test/java/docs/javadsl/Examples.java) { #init-mat }

This is all preparation that we are going to need.

### Flow messages to AWS Lambda

Now we can stream AWS Java SDK Lambda `InvokeRequest` to AWS Lambda functions
@scaladoc[AwsLambdaFlow](akka.stream.alpakka.awslambda.scaladsl.AwsLambdaFlow$) factory.

Scala
: @@snip (/awslambda/src/test/scala/docs/scaladsl/Examples.scala) { #run }

Java
: @@snip (/awslambda/src/test/java/docs/javadsl/Examples.java) { #run }

#### AwsLambdaFlow configuration

Options:

 - `parallelism` - Number of parallel executions. Should be less or equal to number of threads in ExecutorService for AWSLambdaAsyncClient

@@@ warning
AWSLambdaAsyncClient uses blocking http client for Lambda function invocation, make sure that there is enough threads for execution in AWSLambdaAsyncClient.
@@@
