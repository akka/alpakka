# AWS EventBridge

@@@ note { title="Amazon EventBridge" }

Serverless event bus that connects application data from your own apps, SaaS, and AWS services. It has evolved from Amazon CloudWatch Events [official documentation](https://docs.aws.amazon.com/AmazonCloudWatch/latest/events/WhatIsCloudWatchEvents.html). The EventBridge acts as broker so you can set up youw own rules based on which are the events routed into AWS (SNS, SQS, Kinesis, Lambda etc.) or third-party services. 

For more information about AWS EventBridge please visit the [official documentation](https://aws.amazon.com/eventbridge/).

The publish of the events is implemented using the AWS API PUT Events https://docs.aws.amazon.com/eventbridge/latest/userguide/add-events-putevents.html.
The semantics of the publish are that any of the entries inside a Put Request can fail. Response containse information about which entries
were not successfully published.
Currently there are no retries supported apart of configuration provided by the AWS client. 
Adding Support for configurable retry behaviour as part of the connector is possible.

@@@

The AWS EventBridge connector provides Akka Stream sinks for AWS EventBridge event buses.

@@project-info{ projectId="aws-event-bridge" }


## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-aws-event-bridge_$scala.binary.version$
  version=$project.version$
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="aws-event-bridge" }


## Setup

Prepare an @scaladoc[ActorSystem](akka.actor.ActorSystem) and a @scaladoc[Materializer](akka.stream.Materializer).

Scala
: @@snip [snip](/aws-event-bridge/src/test/scala/akka/stream/alpakka/aws/eventbridge/IntegrationTestContext.scala) { #init-system }

Java
: @@snip [snip](/aws-event-bridge/src/test/java/docs/javadsl/EventBridgePublisherTest.java) { #init-system }


This connector requires an implicit @javadoc[EventBridge](software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient) instance to communicate with AWS EventBridge.

It is your code's responsibility to call `close` to free any resources held by the client. In this example it will be called when the actor system is terminated.

Scala
: @@snip [snip](/aws-event-bridge/src/test/scala/akka/stream/alpakka/aws/eventbridge/IntegrationTestContext.scala) { #init-client }

Java
: @@snip [snip](/aws-event-bridge/src/test/java/docs/javadsl/EventBridgePublisherTest.java) { #init-client }

The example above uses @extref:[Akka HTTP](akka-http:) as the default HTTP client implementation. For more details about the HTTP client, configuring request retrying and best practices for credentials, see @ref[AWS client configuration](aws-shared-configuration.md) for more details.


## Publish messages to AWS EventBridge Event Bus

Create a `PutEventsEntry`-accepting sink, publishing to an Event Bus.

Scala
: @@snip [snip](/aws-event-bridge/src/test/scala/docs/scaladsl/EventBridgePublisherSpec.scala) { #run-events-entry }

Java
: @@snip [snip](/aws-event-bridge/src/test/java/docs/javadsl/EventBridgePublisherTest.java) { #run-events-entry }


Create a `PutEventsRequest`-accepting sink, that publishes to an Event Bus.

Scala
: @@snip [snip](/aws-event-bridge/src/test/scala/docs/scaladsl/EventBridgePublisherSpec.scala) { #run-events-request }

Java
: @@snip [snip](/aws-event-bridge/src/test/java/docs/javadsl/EventBridgePublisherTest.java) { #run-events-request }

You can also build flow stages which publish messages to Event Bus and then forward 
@scaladoc[PutEventsResponse](software.amazon.awssdk.services.eventbridge.model.PutEventsResponse) further down the stream.

Flow for PutEventEntry 

Scala
: @@snip [snip](/aws-event-bridge/src/test/scala/docs/scaladsl/EventBridgePublisherSpec.scala) { #flow-events-entry }

Java
: @@snip [snip](/aws-event-bridge/src/test/java/docs/javadsl/EventBridgePublisherTest.java) { #flow-events-entry }

Flow for PutEventsRequest 

Scala
: @@snip [snip](/aws-event-bridge/src/test/scala/docs/scaladsl/EventBridgePublisherSpec.scala) { #flow-events-request }

Java
: @@snip [snip](/aws-event-bridge/src/test/java/docs/javadsl/EventBridgePublisherTest.java) { #flow-events-request }

Flow supporting a list of PutEventEntry objects.

When using the batch mode - `flowSeq` for `PutEventEntry` objects or directly a multiple messages insithe `PutEventsRequest` the eventbridge processing is not all or nothing. Processing of each `PutEventEntry` is evaluated independently in AWS. Retries of the failed messages in the `PutEventsResponse` are not yet implemented.

## Integration testing

For integration testing without touching Amazon EventBrigde, Alpakka uses [Localstack](https://github.com/localstack/localstack), 
which comes as a docker image - and has a corresponding service `amazoneventbridge` in the `docker-compose.yml` file. Which needs to be started before running the integration tests `docker-compose up amazoneventbridge`.

@@@ index

* [retry conf](aws-shared-configuration.md)

@@@
