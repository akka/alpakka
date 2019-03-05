# AWS Lambda

The AWS Lambda Connector provides Akka Flow for AWS Lambda integration.

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

## Sending messages

Flow provided by this connector needs a prepared `LambdaAsyncClient` to be able to invoke lambda functions.

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

Now we can stream AWS Java SDK Lambda `InvokeRequest` to AWS Lambda functions
@scaladoc[AwsLambdaFlow](akka.stream.alpakka.awslambda.scaladsl.AwsLambdaFlow$) factory.

Scala
: @@snip (/awslambda/src/test/scala/docs/scaladsl/Examples.scala) { #run }

Java
: @@snip (/awslambda/src/test/java/docs/javadsl/Examples.java) { #run }

## AwsLambdaFlow configuration

Options:

 - `parallelism` - Number of parallel executions. Should be less or equal to number of threads in ExecutorService for LambdaAsyncClient 

