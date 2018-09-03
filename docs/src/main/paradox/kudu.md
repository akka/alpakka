# Apache Kudu

A flow and a composite sink to write element in [Kudu](http://kudu.apache.org).

Apache Kudu is a free and open source column-oriented data store of the Apache Hadoop ecosystem.

### Reported issues

[Tagged issues at Github](https://github.com/akka/alpakka/labels/p%3Akudu)

## Configuration

Build a converter and a tableSetting.

Converter will map the domain object to Kudu row.

Scala
:   @@snip [snip](/kudu/src/test/scala/docs/scaladsl/KuduTableSpec.scala) { #create-converter }

Java
:   @@snip [snip](/kudu/src/test/java/docs/javadsl/KuduTableTest.java) { #create-converter }

The table will be created on demand.

Scala
:   @@snip [snip](/kudu/src/test/scala/docs/scaladsl/KuduTableSpec.scala) { #create-settings }

Java
:   @@snip [snip](/kudu/src/test/java/docs/javadsl/KuduTableTest.java) { #create-settings }


## Writing to Kudu in Flow

Scala
: @@snip [snip](/kudu/src/test/scala/docs/scaladsl/KuduTableSpec.scala) { #flow }

Java
: @@snip [snip](/kudu/src/test/java/docs/javadsl/KuduTableTest.java) { #flow }


## Writing to Kudu with a Sink

Scala
: @@snip [snip](/kudu/src/test/scala/docs/scaladsl/KuduTableSpec.scala) { #sink }

Java
: @@snip [snip](/kudu/src/test/java/docs/javadsl/KuduTableTest.java) { #sink }
