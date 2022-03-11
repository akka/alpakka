# Integration Patterns

Many [Enterprise Integration Patterns](https://www.enterpriseintegrationpatterns.com/patterns/messaging/toc.html) can be implemented with Akka Streams 
(see @extref:[Akka Streams documentation](akka:stream/index.html)).

# PassThrough

Use PassThroughFlow when you have a message that should be used in a 
flow that transform it but you want to maintain the original message for
another following flow.
For example when consuming messages from Kafka (CommittableMessage), 
the message can be used inside a flow (transform it, save it inside a database, ...)
 and then you need again the original message to commit the offset.

It can be used whenever you have 2 flows:

- Flow1 that takes a message `A` and returns `B`
- Flow2 that takes a message `A` and return `C`

If you want to execute first Flow1 and then Flow2 you need a way to 
maintain/passthrough message `A`. 

```
                    a=>transform=>a1
                   /                 \
                  /                   \
a=>(a, a)=>unzip -                     zip=>(a1, a)=> a
                  \                   /
                   \                 /
                    --------a--------
```

Scala
: @@snip [snip](/doc-examples/src/test/scala/akka/stream/alpakka/eip/scaladsl/PassThroughExamples.scala) { #PassThrough }

Java
: @@snip [snip](/doc-examples/src/test/java/akka/stream/alpakka/eip/javadsl/PassThroughExamples.java) { #PassThrough }

A sample usage:
 
Scala
: @@snip [snip](/doc-examples/src/test/scala/akka/stream/alpakka/eip/scaladsl/PassThroughExamples.scala) { #PassThroughTuple }

Java
: @@snip [snip](/doc-examples/src/test/java/akka/stream/alpakka/eip/javadsl/PassThroughExamples.java) { #PassThroughTuple }

Using `Keep` you can choose what it the return value:

- `PassThroughFlow(passThroughMe, Keep.right)`: to only output the original message
- `PassThroughFlow(passThroughMe, Keep.both)`: to output both values as a `Tuple`
- `Keep.left`/`Keep.none`: are not very useful in this use case, there isn't a pass-through ...

You can also write your own output function to combine in different ways the two outputs.

Scala
: @@snip [snip](/doc-examples/src/test/scala/akka/stream/alpakka/eip/scaladsl/PassThroughExamples.scala) { #PassThroughWithKeep }

Java
: @@snip [snip](/doc-examples/src/test/java/akka/stream/alpakka/eip/javadsl/PassThroughExamples.java) { #PassThroughWithKeep }

This pattern is useful when integrating Alpakka connectors together. Here an example with Kafka:

Scala
: @@snip [snip](/doc-examples/src/test/scala/akka/stream/alpakka/eip/scaladsl/PassThroughExamples.scala) { #passThroughKafkaFlow }

Java
: @@snip [snip](/doc-examples/src/test/java/akka/stream/alpakka/eip/javadsl/PassThroughExamples.java) { #passThroughKafkaFlow }
