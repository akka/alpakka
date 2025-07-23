# AWS Lambda

The AWS Lambda connector provides Akka Flow for AWS Lambda integration.

For more information about AWS Lambda please visit the [AWS lambda documentation](https://docs.aws.amazon.com/lambda/index.html).

@@project-info{ projectId="awslambda" }

## Artifacts

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

Additionally, add the dependencies as below.

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-awslambda_$scala.binary.version$
  version=$project.version$
  symbol2=AkkaVersion
  value2=$akka.version$
  group2=com.typesafe.akka
  artifact2=akka-stream_$scala.binary.version$
  version2=AkkaVersion
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="awslambda" }

## Setup

The flow provided by this connector needs a prepared @javadoc[LambdaAsyncClient](software.amazon.awssdk.services.lambda.LambdaAsyncClient) to be able to invoke lambda functions.

Scala
: @@snip (/awslambda/src/test/scala/docs/scaladsl/Examples.scala) { #init-client }

Java
: @@snip (/awslambda/src/test/java/docs/javadsl/Examples.java) { #init-client }

The example above uses @extref:[Akka HTTP](akka-http:) as the default HTTP client implementation. For more details about the HTTP client, configuring request retrying and best practices for credentials, see @ref[AWS client configuration](aws-shared-configuration.md) for more details.

We will need an @apidoc[akka.actor.ActorSystem].

Scala
: @@snip (/awslambda/src/test/scala/docs/scaladsl/Examples.scala) { #init-sys }

Java
: @@snip (/awslambda/src/test/java/docs/javadsl/Examples.java) { #init-sys }

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

* [retry conf](aws-shared-configuration.md)

@@@
