# AMQP

The AMQP connector provides Akka Stream sources and sinks to connect to AMQP 0.9.1 servers (RabbitMQ, OpenAMQ, etc.).

AMQP 1.0 is currently not supported (Qpid, ActiveMQ, Solace, etc.).

@@project-info{ projectId="amqp" }

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-amqp_$scala.binary.version$
  version=$project.version$
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="amqp" }

## Connecting to server

All the AMQP connectors are configured using a @apidoc[AmqpConnectionProvider] and a list of @apidoc[amqp.Declaration]

There are several types of @apidoc[AmqpConnectionProvider]:

* @apidoc[AmqpLocalConnectionProvider$] which connects to the default localhost. It creates a new connection for each stage.
* @apidoc[AmqpUriConnectionProvider$] which connects to the given AMQP URI. It creates a new connection for each stage.
* @apidoc[AmqpDetailsConnectionProvider$] which supports more fine-grained configuration. It creates a new connection for each stage.
* @apidoc[AmqpConnectionFactoryConnectionProvider$] which takes a raw [ConnectionFactory](https://rabbitmq.github.io/rabbitmq-java-client/api/current/com/rabbitmq/client/ConnectionFactory.html). It creates a new connection for each stage.
* @apidoc[AmqpCachedConnectionProvider$] which receive any other provider as parameter and caches the connection it provides to be used in all stages. By default it closes the connection whenever the last stage using the provider stops. Optionally, it takes `automaticRelease` boolean parameter so the connection is not automatically release and the user have to release it explicitly.

## Sending messages

First define a queue name and the declaration of the queue that the messages will be sent to.

Scala
: @@snip [snip](/amqp/src/test/scala/docs/scaladsl/AmqpDocsSpec.scala) { #queue-declaration }

Java
: @@snip [snip](/amqp/src/test/java/docs/javadsl/AmqpDocsTest.java) { #queue-declaration }

Here we used @apidoc[QueueDeclaration] configuration class to create a queue declaration.

### With flow

Similarly as with Sink, the first step is to create Flow which accepts @apidoc[amqp.WriteMessage]s and forwards it's content to the AMQP server. Flow emits @apidoc[amqp.WriteResult]s informing about publication result (see below for summary of delivery guarantees for different Flow variants).

@apidoc[AmqpFlow$] is a collection of factory methods that facilitates creation of flows. Here we created a *simple* sink, which means that we are able to pass `ByteString`s to the sink instead of wrapping data into @apidoc[amqp.WriteMessage]s.

Last step is to @extref:[materialize](akka:stream/stream-flows-and-basics.html) and run the flow we have created.

Scala
: @@snip [snip](/amqp/src/test/scala/docs/scaladsl/AmqpDocsSpec.scala) { #create-flow }

Java
: @@snip [snip](/amqp/src/test/java/docs/javadsl/AmqpDocsTest.java) { #create-flow }

Various variants of AMQP flow offer different delivery and ordering guarantees:

| AMQP flow factory                  | Description                                                                                          |
| ---------------------------------- | ---------------------------------------------------------------------------------------------------- |
| AmqpFlow.apply                     | The most basic type of flow. Does not impose delivery guarantees, messages are published in a fire-and-forget manner. Emitted results have `confirmed` always set to true.
| AmqpFlow.withConfirm          | Variant that uses asynchronous confirmations. Maximum number of messages simultaneously waiting for confirmation before signaling backpressure is configured with a `bufferSize` parameter. Emitted results preserve the order of messages pulled from upstream - due to that restriction this flow is expected to be slightly less effective than it's unordered counterpart.
| AmqpFlow.withUnorderedConfirm | The same as `AmqpFlow.withConfirm` with the exception of ordering guarantee - results are emitted downstream as soon as confirmation is received, meaning that there is no ordering guarantee of any sort.

For @apidoc[FlowWithContext$] counterparts of above flows see @apidoc[AmqpFlowWithContext$].

@@@ warning
`AmqpFlow.withConfirm` and `AmqpFlow.withUnorderedConfirm` are implemented using RabbitMQ's extension to AMQP protocol ([Publisher Confirms](https://www.rabbitmq.com/confirms.html#publisher-confirms)), therefore they are not intended to work with another AMQP brokers.
@@@

### With sink

Create a sink, that accepts and forwards @apidoc[akka.util.ByteString]s to the AMQP server.

@apidoc[AmqpSink$] is a collection of factory methods that facilitates creation of sinks. Here we created a *simple* sink, which means that we are able to pass `ByteString`s to the sink instead of wrapping data into @apidoc[amqp.WriteMessage]s.

Last step is to @extref:[materialize](akka:stream/stream-flows-and-basics.html) and run the sink we have created.

Scala
: @@snip [snip](/amqp/src/test/scala/docs/scaladsl/AmqpDocsSpec.scala) { #create-sink }

Java
: @@snip [snip](/amqp/src/test/java/docs/javadsl/AmqpDocsTest.java) { #create-sink }

## Receiving messages

Create a source using the same queue declaration as before.

The `bufferSize` parameter controls the maximum number of messages to prefetch from the AMQP server.

Run the source and take the same amount of messages as we previously sent to it.


Scala
: @@snip [snip](/amqp/src/test/scala/docs/scaladsl/AmqpDocsSpec.scala) { #create-source }

Java
: @@snip [snip](/amqp/src/test/java/docs/javadsl/AmqpDocsTest.java) { #create-source }

This is how you send and receive message from AMQP server using this connector.

## Using Pub/Sub

Instead of sending messages directly to queues, it is possible to send messages to an exchange and then provide instructions to the AMQP server what to do with incoming messages. We are going to use the *fanout* type of the exchange, which enables message broadcasting to multiple consumers. We are going to do that by using an exchange declaration for the sink and all of the sources.

Scala
: @@snip [snip](/amqp/src/test/scala/docs/scaladsl/AmqpDocsSpec.scala) { #exchange-declaration }

Java
: @@snip [snip](/amqp/src/test/java/docs/javadsl/AmqpDocsTest.java) { #exchange-declaration }

The sink for the exchange is created in a very similar way.

Scala
: @@snip [snip](/amqp/src/test/scala/docs/scaladsl/AmqpDocsSpec.scala) { #create-exchange-sink }

Java
: @@snip [snip](/amqp/src/test/java/docs/javadsl/AmqpDocsTest.java) { #create-exchange-sink }

For the source, we are going to create multiple sources and merge them using @extref:[Akka Streams operators](akka:stream/operators/index.html).

Scala
: @@snip [snip](/amqp/src/test/scala/docs/scaladsl/AmqpDocsSpec.scala) { #create-exchange-source }

Java
: @@snip [snip](/amqp/src/test/java/docs/javadsl/AmqpDocsTest.java) { #create-exchange-source }

We merge all sources into one and add the index of the source to all incoming messages, so we can distinguish which source the incoming message came from.

Such sink and source can be started the same way as in the previous example.

## Using rabbitmq as an RPC mechanism

If you have remote workers that you want to incorporate into a stream, you can do it using rabbit RPC workflow [RabbitMQ RPC](https://www.rabbitmq.com/tutorials/tutorial-six-java.html)

Scala
: @@snip [snip](/amqp/src/test/scala/docs/scaladsl/AmqpDocsSpec.scala) { #create-rpc-flow }

Java
: @@snip [snip](/amqp/src/test/java/docs/javadsl/AmqpDocsTest.java) { #create-rpc-flow }


## Acknowledging messages downstream

Committable sources return @apidoc[CommittableReadResult] which wraps the @apidoc[amqp.ReadResult] and exposes the methods `ack` and `nack`.

Use `ack` to acknowledge the message back to RabbitMQ. `ack` takes an optional boolean parameter `multiple` indicating whether you are acknowledging the individual message or all the messages up to it.

Use `nack` to reject a message. Apart from the `multiple` argument, `nack` takes another optional boolean parameter indicating whether the item should be requeued or not.

Scala
: @@snip [snip](/amqp/src/test/scala/docs/scaladsl/AmqpDocsSpec.scala) { #create-source-withoutautoack }

Java
: @@snip [snip](/amqp/src/test/java/docs/javadsl/AmqpDocsTest.java) { #create-source-withoutautoack }
