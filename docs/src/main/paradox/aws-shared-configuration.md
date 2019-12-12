# Shared AWS client configuration

## Underlying HTTP client

Scala
: @@snip [snip](/sqs/src/test/scala/akka/stream/alpakka/sqs/scaladsl/DefaultTestContext.scala) { #init-client }

Java
: @@snip [snip](/sqs/src/test/java/akka/stream/alpakka/sqs/javadsl/BaseSqsTest.java) { #init-client }

The example snippets show how the AWS clients are setup to use @extref:[Akka HTTP](akka-http:) as the default HTTP client implementation via the thin adapter library [AWS Akka-Http SPI implementation](https://github.com/matsluni/aws-spi-akka-http). By setting the `httpClient` explicitly (as above) the Akka actor system is reused.  If it is not set explicitly then a separate actor system will be created internally.


### Using Netty

It is possible to configure the use of Netty instead, which is Amazon's default. Add an appropriate Netty version to the dependencies and configure @javadoc[NettyNioAsyncHttpClient](software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient).

Scala
: @@snip [snip](/sqs/src/test/scala/docs/scaladsl/SqsSourceSpec.scala) { #init-custom-client }

Java
: @@snip [snip](/sqs/src/test/java/docs/javadsl/SqsSourceTest.java) { #init-custom-client }

Please make sure to configure a big enough thread pool for the Netty client to avoid resource starvation. This is especially important,
if you share the client between multiple Sources, Sinks and Flows. For the SQS Sinks and Sources the sum of all
`parallelism` (Source) and `maxInFlight` (Sink) must be less than or equal to the thread pool size.


## AWS retry configuration

The AWS SDK 2 supports request retrying with exponential backoff.

The request retry behaviour is configurable via the @javadoc[SdkDefaultClientBuilder.overrideConfiguration](software.amazon.awssdk.core.client.builder.SdkDefaultClientBuilder#overrideConfiguration-software.amazon.awssdk.core.client.config.ClientOverrideConfiguration-) method by using the @javadoc[RetryPolicy](software.amazon.awssdk.core.retry.RetryPolicy).

Scala
: @@snip [snip](/dynamodb/src/test/scala/docs/scaladsl/RetrySpec.scala) { #awsRetryConfiguration }

Java
: @@snip [snip](/dynamodb/src/test/java/docs/javadsl/RetryTest.java) { #awsRetryConfiguration }


## AWS Access Keys

Do not encode AWS Access Keys in your source code or in static configuration. Please refer to [Best Practices for Managing AWS Access Keys](https://docs.aws.amazon.com/general/latest/gr/aws-access-keys-best-practices.html) for proper AWS Access Key management.
