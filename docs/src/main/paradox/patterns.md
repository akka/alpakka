# Integration Patterns

Many [Enterprise Integration Patterns](http://www.eaipatterns.com/toc.html) can be implemented with Akka Streams 
(see [Java documentation](http://doc.akka.io/docs/akka/2.4/java/stream/index.html) or [Scala documentation](http://doc.akka.io/docs/akka/2.4/scala/stream/index.html)).


# Splitter

You can achieve a [Splitter](http://www.enterpriseintegrationpatterns.com/patterns/messaging/Sequencer.html) as described in  [EIP](http://www.enterpriseintegrationpatterns.com) using out of the box Akka Streams dsl.

![Splitter](http://www.enterpriseintegrationpatterns.com/img/Sequencer.gif)

## Simple Splitter

Scala
: @@snip (../../../../eip/src/test/scala/akka/stream/alpakka/eip/scaladsl/SplitterExamples.scala) { #Simple-Split }

Java
: @@snip (../../../../eip/src/test/java/akka/stream/alpakka/eip/javadsl/SplitterExamples.java) { #Simple-Split }

## Spliter + Aggregator

Sometimes it's very useful to split a message and aggregate it's "sub-messages" into a new message (A combination of [Splitter](http://www.enterpriseintegrationpatterns.com/patterns/messaging/Sequencer.html) and [Aggregator](http://www.enterpriseintegrationpatterns.com/patterns/messaging/Aggregator.html)) 

Scala
: @@snip (../../../../eip/src/test/scala/akka/stream/alpakka/eip/scaladsl/SplitterExamples.scala) { #Aggregate-Split }

Java
: @@snip (../../../../eip/src/test/java/akka/stream/alpakka/eip/javadsl/SplitterExamples.java) { #Aggregate-Split }


 
TODO: Create documentation pages for typical integration patterns and some might deserve a higher level component that is implemented in Alpakka. [Contributions](https://github.com/akka/alpakka/blob/master/CONTRIBUTING.md) are very welcome.
[Creating an issue](https://github.com/akka/alpakka/issues) for discussion is a good first step for such contributions.