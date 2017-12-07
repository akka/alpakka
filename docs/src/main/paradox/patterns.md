# Integration Patterns

Many [Enterprise Integration Patterns](http://www.eaipatterns.com/toc.html) can be implemented with Akka Streams 
(see @extref[Akka Streams documentation](akka-docs:stream/index.html)).


# Splitter

You can achieve a [Splitter](http://www.enterpriseintegrationpatterns.com/patterns/messaging/Sequencer.html) as described in  [EIP](http://www.enterpriseintegrationpatterns.com) using out of the box Akka Streams dsl.

![Splitter](img/Sequencer.gif)

## Simple Splitter

Let's say that we have a stream containing strings. Each string contains a few numbers separated by "-". We want to create out of this a stream that only contains the numbers. 

Scala
: @@snip (../../test/scala/akka/stream/alpakka/eip/scaladsl/SplitterExamples.scala) { #Simple-Split }

Java
: @@snip (../../test/java/akka/stream/alpakka/eip/javadsl/SplitterExamples.java) { #Simple-Split }

## Spliter + Aggregator

Sometimes it's very useful to split a message and aggregate it's "sub-messages" into a new message (A combination of [Splitter](http://www.enterpriseintegrationpatterns.com/patterns/messaging/Sequencer.html) and [Aggregator](http://www.enterpriseintegrationpatterns.com/patterns/messaging/Aggregator.html)) 

Let's say that now we want to create a new stream containing the sums of the numbers in each original string. 


Scala
: @@snip (../../test/scala/akka/stream/alpakka/eip/scaladsl/SplitterExamples.scala) { #Aggregate-Split }

Java
: @@snip (../../test/java/akka/stream/alpakka/eip/javadsl/SplitterExamples.java) { #Aggregate-Split }

While in real life this solution if overkill for such a simple problem (you can just do everything in a map), more complex scenarios, involving in particular I/O, will benefit from the fact that you can paralelize sub-streams and get back-pressure for "free".


 
TODO: Create documentation pages for typical integration patterns and some might deserve a higher level component that is implemented in Alpakka. [Contributions](https://github.com/akka/alpakka/blob/master/CONTRIBUTING.md) are very welcome.
[Creating an issue](https://github.com/akka/alpakka/issues) for discussion is a good first step for such contributions.