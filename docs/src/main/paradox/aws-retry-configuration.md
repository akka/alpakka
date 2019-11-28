# AWS retry configuration

The AWS SDK 2 supports request retrying with exponential backoff.

The request retry behaviour is configurable via the @javadoc[SdkDefaultClientBuilder.overrideConfiguration](software.amazon.awssdk.core.client.builder.SdkDefaultClientBuilder#overrideConfiguration-software.amazon.awssdk.core.client.config.ClientOverrideConfiguration-) method by using the @javadoc[RetryPolicy](software.amazon.awssdk.core.retry.RetryPolicy).

Scala
: @@snip [snip](/dynamodb/src/test/scala/docs/scaladsl/RetrySpec.scala) { #awsRetryConfiguration }

Java
: @@snip [snip](/dynamodb/src/test/java/docs/javadsl/RetryTest.java) { #awsRetryConfiguration }
