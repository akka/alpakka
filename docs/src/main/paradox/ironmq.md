# IronMq Connector

The IronMq connector provides an Akka stream source and sink to connect to the [IronMQ](https://www.iron.io/platform/ironmq/) queue.

IronMq is a simple point-to-point queue, but it is possible to implement a fan-out semantic by configure the queue as push
queue and set other queue as subscribers. More information about that could be found on
[IronMq documentation](https://www.iron.io/ironmq-fan-out-support/)

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-ironmq_$scalaBinaryVersion$
  version=$version$
}

## Usage

IronMq can be used either in cloud or on-premise. Either way you need a authentication token and a project ID. These can
be set in the Typesafe config:

@@snip ($alpakka$/ironmq/src/main/resources/reference.conf)

### Consumer

The consumer is poll based one. It will poll every `n` milliseconds, waiting for `m` milliseconds to consume new messages and
will push them to the downstream. All These parameters are configurable by the Typesafe config.

It supports both at-most-once and at-least-once semantics. In the first case the messages are deleted straight away after
been fetched. In the latter case the messages piggy back a Committable object that should be used to commit the message.
Committing the message will cause the message to be deleted from the queue.

The consumer could be instantiated using the @scaladoc[IronMqConsumer](akka.stream.alpakka.ironmq.scaladsl.IronMqConsumer$).
It provides methods to obtain either a `Source[Message, NotUsed]` or `Source[CommittableMessage, NotUsed]`. The first is
for at-most-one semantic, the latter for at-least-once semantic.

### Producer
The producer is very trivial at this time, it does not provide any batching mechanism, but sends messages to IronMq as
soon as they arrive to the stage.

The producer could be instantiated using the @scaladoc[IronMqProducer](akka.stream.alpakka.ironmq.scaladsl.IronMqProducer$).
It provides methods to obtain either a `Flow[PushMessage, Messages.Id, NotUsed]` or a `Sink[PushMessage, NotUsed]`.

The @scaladoc[PushMessage](akka.stream.alpakka.ironmq.PushMessage) allows to specify the delay per individual message. The
message expiration is set a queue level. The @scaladoc[IronMqClient](akka.stream.alpakka.ironmq.IronMqClient) does not
still have all the features to create and update these settings on a queue.

If you are using the producer `Flow` The returned @scaladoc[Messages.Ids](akka.stream.alpakka.ironmq.Messages$$Id) will
contain the ID of the pushed message, that can be used to manipulate the message.

For each `PushMessage` from the upstream you will have exactly one `Message.Id` in downstream in the same order. Regardless
if the producer will implement a batch mechanism in the future.

The producer also provides a Committable aware Flow/Sink as `Flow[(PushMessage, ToCommit), (Message.Id, CommitResult), CommitMat]`.
It can be used to consume a Flow from an IronMq consumer or any other source that provides a commit mechanism.

> Test code requires IronMQ running in the background. You can start it quickly using docker:
>
> `docker-compose up ironauth ironmq`

Scala
:   ```
    sbt
    > ironmq/testOnly *Spec
    ```

Java
:   ```
    sbt
    > ironmq/testOnly *Test
    ```