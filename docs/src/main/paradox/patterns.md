# Integration Patterns

Many [Enterprise Integration Patterns](https://www.enterpriseintegrationpatterns.com/patterns/messaging/toc.html) can be implemented with Akka Streams 
(see @extref:[Akka Streams documentation](akka:stream/index.html)).


# Splitter

You can achieve a [Splitter](https://www.enterpriseintegrationpatterns.com/patterns/messaging/Sequencer.html) as described in  [EIP](https://www.enterpriseintegrationpatterns.com) using out of the box Akka Streams dsl.

![Splitter](img/Sequencer.gif)

## Simple Splitter

Let's say that we have a stream containing strings. Each string contains a few numbers separated by "-". We want to create out of this a stream that only contains the numbers. 

Scala
: @@snip [snip](/doc-examples/src/test/scala/akka/stream/alpakka/eip/scaladsl/SplitterExamples.scala) { #Simple-Split }

Java
: @@snip [snip](/doc-examples/src/test/java/akka/stream/alpakka/eip/javadsl/SplitterExamples.java) { #Simple-Split }

## Splitter + Aggregator

Sometimes it's very useful to split a message and aggregate it's "sub-messages" into a new message (A combination of [Splitter](https://www.enterpriseintegrationpatterns.com/patterns/messaging/Sequencer.html) and [Aggregator](https://www.enterpriseintegrationpatterns.com/patterns/messaging/Aggregator.html)) 

Let's say that now we want to create a new stream containing the sums of the numbers in each original string. 


Scala
: @@snip [snip](/doc-examples/src/test/scala/akka/stream/alpakka/eip/scaladsl/SplitterExamples.scala) { #Aggregate-Split }

Java
: @@snip [snip](/doc-examples/src/test/java/akka/stream/alpakka/eip/javadsl/SplitterExamples.java) { #Aggregate-Split }

While in real life this solution if overkill for such a simple problem (you can just do everything in a map), more complex scenarios, involving in particular I/O, will benefit from the fact that you can paralelize sub-streams and get back-pressure for "free".

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
