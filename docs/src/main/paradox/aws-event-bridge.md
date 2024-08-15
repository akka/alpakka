# AWS EventBridge

@@@ note { title="Amazon EventBridge" }

Amazon EventBridge is a serverless event bus that allows your applications to asynchronously consume events from 3rd party SaaS offerings, AWS services, and other applications in your own infrastructure. 
It evolved from Amazon CloudWatch Events ([official documentation](https://docs.aws.amazon.com/AmazonCloudWatch/latest/events/WhatIsCloudWatchEvents.html)). 
The EventBridge acts as broker that you can configure with your own rules to route events to the correct service. 

For more information about AWS EventBridge please visit the [official documentation](https://aws.amazon.com/eventbridge/).

The publishing of the events is implemented using the [AWS PUT Events API](https://docs.aws.amazon.com/eventbridge/latest/userguide/add-events-putevents.html).

When publishing events any of the entries inside of the Put request can fail. 
The response contains information about which entries were not successfully published.
Currently, there are no retries supported apart from the configuration provided to the EventBridge client. 

Adding Support for configurable retry behaviour as part of the connector may be part of a future release.

By default the client will publish to a default event bus, but normally you should publish to a specific event bus that you create.

An event bus name is defined per event in a [PutEventsRequestEntry](https://docs.aws.amazon.com/eventbridge/latest/APIReference/API_PutEventsRequestEntry.html) object.
It would be possible to define helper flows/sinks with default values such as source and `eventBusName`. 
The `detail` is JSON as a string and `detailType` is the name of the event for rule matching.

@@@

The Alpakka AWS EventBridge connector provides Akka Stream flows and sinks to publish to AWS EventBridge event buses.


@@project-info{ projectId="aws-event-bridge" }


## Artifacts

The Akka dependencies are available from Akka's library repository. To access them there, you need to configure the URL for this repository.

@@repository [sbt,Maven,Gradle] {
id="akka-repository"
name="Akka library repository"
url="https://repo.akka.io/maven"
}

Additionally, add the dependencies as below.

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-aws-event-bridge_$scala.binary.version$
  version=$project.version$
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="aws-event-bridge" }


## Setup

Prepare an @scaladoc[ActorSystem](akka.actor.ActorSystem).

Scala
: @@snip [snip](/aws-event-bridge/src/test/scala/akka/stream/alpakka/aws/eventbridge/IntegrationTestContext.scala) { #init-system }

Java
: @@snip [snip](/aws-event-bridge/src/test/java/docs/javadsl/EventBridgePublisherTest.java) { #init-system }


This connector requires an @javadoc[EventBridge](software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient) instance to communicate with AWS EventBridge.


It is your code's responsibility to call `close` to free any resources held by the client. In this example it will be called when the actor system is terminated.

Scala
: @@snip [snip](/aws-event-bridge/src/test/scala/akka/stream/alpakka/aws/eventbridge/IntegrationTestContext.scala) { #init-client }

Java
: @@snip [snip](/aws-event-bridge/src/test/java/docs/javadsl/EventBridgePublisherTest.java) { #init-client }

The example above uses @extref:[Akka HTTP](akka-http:) as the default HTTP client implementation. For more details about the HTTP client, configuring request retrying and best practices for credentials, see @ref[AWS client configuration](aws-shared-configuration.md).



## Publish messages to AWS EventBridge Event Bus

Create a `PutEventsEntry`-accepting sink, publishing to an event bus.


Scala
: @@snip [snip](/aws-event-bridge/src/test/scala/docs/scaladsl/EventBridgePublisherSpec.scala) { #run-events-entry }

Java
: @@snip [snip](/aws-event-bridge/src/test/java/docs/javadsl/EventBridgePublisherTest.java) { #run-events-entry }


Create a sink that accepts `PutEventsRequestEntries` to be published to an Event Bus.


Scala
: @@snip [snip](/aws-event-bridge/src/test/scala/docs/scaladsl/EventBridgePublisherSpec.scala) { #run-events-request }

Java
: @@snip [snip](/aws-event-bridge/src/test/java/docs/javadsl/EventBridgePublisherTest.java) { #run-events-request }

You can also build flow stages which publish messages to Event Bus and then forward 
@javadoc[PutEventsResponse](software.amazon.awssdk.services.eventbridge.model.PutEventsResponse) further down the stream.

Flow for `PutEventEntry`.

Scala
: @@snip [snip](/aws-event-bridge/src/test/scala/docs/scaladsl/EventBridgePublisherSpec.scala) { #flow-events-entry }

Java
: @@snip [snip](/aws-event-bridge/src/test/java/docs/javadsl/EventBridgePublisherTest.java) { #flow-events-entry }

Flow for `PutEventsRequest`.

Scala
: @@snip [snip](/aws-event-bridge/src/test/scala/docs/scaladsl/EventBridgePublisherSpec.scala) { #flow-events-request }

Java
: @@snip [snip](/aws-event-bridge/src/test/java/docs/javadsl/EventBridgePublisherTest.java) { #flow-events-request }

Flow supporting a list of `PutEventEntry` objects.

Messages published in a batch using @apidoc[EventBridgePublisher.flowSeq](EventBridgePublisher$) are not published in an "all or nothing" manner. Event Bridge will process each event independently. Retries of the failed messages in the `PutEventsResponse` are not yet implemented.


## Integration testing

For integration testing without connecting directly to Amazon EventBridge, Alpakka uses [Localstack](https://github.com/localstack/localstack), which comes as a docker image - and has a corresponding service `amazoneventbridge` in the `docker-compose.yml` file. Which needs to be started before running the integration tests `docker compose up amazoneventbridge`.

@@@ index

* [retry conf](aws-shared-configuration.md)

@@@
