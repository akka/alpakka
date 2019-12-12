# AWS Lambda

The AWS Lambda connector provides Akka Flow for AWS Lambda integration.

For more information about AWS Lambda please visit the [AWS lambda documentation](https://aws.amazon.com/documentation/lambda/).

@@project-info{ projectId="awslambda" }

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-awslambda_$scala.binary.version$
  version=$project.version$
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="awslambda" }

## Setup

Flow provided by this connector needs a prepared @javadoc[LambdaAsyncClient](software.amazon.awssdk.services.lambda.LambdaAsyncClient) to be able to invoke lambda functions.

Scala
: @@snip (/awslambda/src/test/scala/docs/scaladsl/Examples.scala) { #init-client }

Java
: @@snip (/awslambda/src/test/java/docs/javadsl/Examples.java) { #init-client }

This connector is setup to use @extref:[Akka HTTP](akka-http:) as the default HTTP client implementation via the thin adapter library [AWS Akka-Http SPI implementation](https://github.com/matsluni/aws-spi-akka-http). By setting the `httpClient` explicitly (as above) the Akka actor system is reused.  If it is not set explicitly then a separate actor system will be created internally.

The client has built-in support for retrying with exponential backoff, see @ref[AWS Retry configuration](aws-retry-configuration.md) for more details.

It is possible to configure the use of Netty instead, which is Amazon's default. Add an appropriate Netty version to the dependencies and configure @javadoc[NettyNioAsyncHttpClient](software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient).

We will also need an @scaladoc[ActorSystem](akka.actor.ActorSystem) and an @scaladoc[ActorMaterializer](akka.stream.ActorMaterializer).

Scala
: @@snip (/awslambda/src/test/scala/docs/scaladsl/Examples.scala) { #init-mat }

Java
: @@snip (/awslambda/src/test/java/docs/javadsl/Examples.java) { #init-mat }

This is all preparation that we are going to need.

## Sending messages

Now we can stream AWS Java SDK Lambda `InvokeRequest` to AWS Lambda functions
@apidoc[AwsLambdaFlow$] factory.

Scala
: @@snip (/awslambda/src/test/scala/docs/scaladsl/Examples.scala) { #run }

Java
: @@snip (/awslambda/src/test/java/docs/javadsl/Examples.java) { #run }

## AwsLambdaFlow configuration

Options:

 - `parallelism` - Number of parallel executions. Should be less or equal to number of threads in ExecutorService for LambdaAsyncClient 

@@@ index

* [retry conf](aws-retry-configuration.md)

@@@
