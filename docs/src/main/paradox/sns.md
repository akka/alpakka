# AWS SNS

The AWS SNS connector provides an Akka Stream Flow and Sink for push notifications through AWS SNS.

For more information about AWS SNS please visit the [official documentation](https://docs.aws.amazon.com/sns/index.html).

@@project-info{ projectId="sns" }

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-sns_$scala.binary.version$
  version=$project.version$
  symbol2=AkkaVersion
  value2=$akka.version$
  group2=com.typesafe.akka
  artifact2=akka-stream_$scala.binary.version$
  version2=AkkaVersion
  symbol3=AkkaHttpVersion
  value3=$akka-http.version$
  group3=com.typesafe.akka
  artifact3=akka-http_$scala.binary.version$
  version3=AkkaHttpVersion
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="sns" }


## Setup

This connector requires an @scala[implicit] @javadoc[SnsAsyncClient](software.amazon.awssdk.services.sns.SnsAsyncClient) instance to communicate with AWS SQS.

It is your code's responsibility to call `close` to free any resources held by the client. In this example it will be called when the actor system is terminated.

Scala
: @@snip [snip](/sns/src/test/scala/akka/stream/alpakka/sns/IntegrationTestContext.scala) { #init-client }

Java
: @@snip [snip](/sns/src/test/java/docs/javadsl/SnsPublisherTest.java) { #init-client }

The example above uses @extref:[Akka HTTP](akka-http:) as the default HTTP client implementation. For more details about the HTTP client, configuring request retrying and best practices for credentials, see @ref[AWS client configuration](aws-shared-configuration.md) for more details.

We will also need an @apidoc[akka.actor.ActorSystem].

Scala
: @@snip [snip](/sns/src/test/scala/akka/stream/alpakka/sns/IntegrationTestContext.scala) { #init-system }

Java
: @@snip [snip](/sns/src/test/java/docs/javadsl/SnsPublisherTest.java) { #init-system }

This is all preparation that we are going to need.

## Publish messages to an SNS topic

Now we can publish a message to any SNS topic where we have access to by providing the topic ARN to the
@apidoc[SnsPublisher$] Flow or Sink factory method.

### Using a Flow

Scala
: @@snip [snip](/sns/src/test/scala/docs/scaladsl/SnsPublisherSpec.scala) { #use-flow }

Java
: @@snip [snip](/sns/src/test/java/docs/javadsl/SnsPublisherTest.java) { #use-flow }

As you can see, this would publish the messages from the source to the specified AWS SNS topic.
After a message has been successfully published, a
@javadoc[PublishResult](software.amazon.awssdk.services.sns.model.PublishRequest)
will be pushed downstream.

### Using a Sink

Scala
: @@snip [snip](/sns/src/test/scala/docs/scaladsl/SnsPublisherSpec.scala) { #use-sink }

Java
: @@snip [snip](/sns/src/test/java/docs/javadsl/SnsPublisherTest.java) { #use-sink }

As you can see, this would publish the messages from the source to the specified AWS SNS topic.

@@@ index

* [retry conf](aws-shared-configuration.md)

@@@
