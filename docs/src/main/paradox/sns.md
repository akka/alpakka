# AWS SNS

The AWS SNS connector provides an Akka Stream Flow and Sink for push notifications through AWS SNS.

For more information about AWS SNS please visit the [official documentation](https://aws.amazon.com/documentation/sns/).

@@project-info{ projectId="sns" }

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-sns_$scala.binary.version$
  version=$project.version$
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="sns" }


## Setup

This connector requires an implicit @javadoc[SnsAsyncClient](software.amazon.awssdk.services.sns.SnsAsyncClient) instance to communicate with AWS SQS.

It is your code's responsibility to call `close` to free any resources held by the client. In this example it will be called when the actor system is terminated.

Scala
: @@snip [snip](/sns/src/test/scala/akka/stream/alpakka/sns/IntegrationTestContext.scala) { #init-client }

Java
: @@snip [snip](/sns/src/test/java/docs/javadsl/SnsPublisherTest.java) { #init-client }

Alpakka SQS and SNS are set up to use @extref:[Akka HTTP](akka-http:) as default HTTP client via the thin adapter library [AWS Akka-Http SPI implementation](https://github.com/matsluni/aws-spi-akka-http). By setting the `httpClient` explicitly (as above) the Akka actor system is reused, if not set explicitly a separate actor system will be created internally.

It is possible to configure the use of Netty instead, which is Amazon's default. Add an appropriate Netty version to the dependencies and configure @javadoc[NettyNioAsyncHttpClient](software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient).

We will also need an @scaladoc[ActorSystem](akka.actor.ActorSystem) and an @scaladoc[ActorMaterializer](akka.stream.ActorMaterializer).

Scala
: @@snip [snip](/sns/src/test/scala/akka/stream/alpakka/sns/IntegrationTestContext.scala) { #init-system }

Java
: @@snip [snip](/sns/src/test/java/docs/javadsl/SnsPublisherTest.java) { #init-system }

This is all preparation that we are going to need.

## Publish messages to an SNS topic

Now we can publish a message to any SNS topic where we have access to by providing the topic ARN to the
@scaladoc[SnsPublisher](akka.stream.alpakka.sns.scaladsl.SnsPublisher$) Flow or Sink factory method.

### Using a Flow

Scala
: @@snip [snip](/sns/src/test/scala/docs/scaladsl/SnsPublisherSpec.scala) { #use-flow }

Java
: @@snip [snip](/sns/src/test/java/docs/javadsl/SnsPublisherTest.java) { #use-flow }

As you can see, this would publish the messages from the source to the specified AWS SNS topic.
After a message has been successfully published, a
[PublishResult](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/sns/model/PublishResult.html)
will be pushed downstream.

### Using a Sink

Scala
: @@snip [snip](/sns/src/test/scala/docs/scaladsl/SnsPublisherSpec.scala) { #use-sink }

Java
: @@snip [snip](/sns/src/test/java/docs/javadsl/SnsPublisherTest.java) { #use-sink }

As you can see, this would publish the messages from the source to the specified AWS SNS topic.
