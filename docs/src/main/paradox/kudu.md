# Apache Kudu

A flow and a composite sink to write element in [Kudu](http://kudu.apache.org).

Apache Kudu is a free and open source column-oriented data store of the Apache Hadoop ecosystem.

### Reported issues

[Tagged issues at Github](https://github.com/akka/alpakka/labels/p%3Akudu)

# Usage

Build a converter and a tableSetting.

Converter will map the domain object to Kudu row.

scala
:   @@snip [snip](/kudu/src/test/scala/akka/stream/alpakka/kudu/scaladsl/KuduStageSpec.scala) { #create-converter }

java
:   @@snip [snip](/kudu/src/test/java/akka/stream/alpakka/kudu/javadsl/KuduStageTest.java) { #create-converter }

Table will be created on demand.

scala
:   @@snip [snip](/kudu/src/test/scala/akka/stream/alpakka/kudu/scaladsl/KuduStageSpec.scala) { #create-settings }

java
:   @@snip [snip](/kudu/src/test/java/akka/stream/alpakka/kudu/javadsl/KuduStageTest.java) { #create-settings }

### Flow usage

scala
: @@snip [snip](/kudu/src/test/scala/akka/stream/alpakka/kudu/scaladsl/KuduStageSpec.scala) { #flow }

java
: @@snip [snip](/kudu/src/test/java/akka/stream/alpakka/kudu/javadsl/KuduStageTest.java) { #flow }


### Sink usage

scala
: @@snip [snip](/kudu/src/test/scala/akka/stream/alpakka/kudu/scaladsl/KuduStageSpec.scala) { #sink }

java
: @@snip [snip](/kudu/src/test/java/akka/stream/alpakka/kudu/javadsl/KuduStageTest.java) { #sink }