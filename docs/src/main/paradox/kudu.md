# Apache Kudu

The Alpakka Kudu connector supports writing to [Apache Kudu](http://kudu.apache.org) tables.

Apache Kudu is a free and open source column-oriented data store in the Apache Hadoop ecosystem.

### Reported issues

[Tagged issues at Github](https://github.com/akka/alpakka/labels/p%3Akudu)

## Configuration

To connect to Kudu

1. Create a Kudu client and manage its life-cycle
1. Describe the Kudu `Schema` (@javadoc[API](org.apache.kudu.Schema))
1. Define a converter function to map your data type to a `PartialRow` (@javadoc[API](org.apache.kudu.client.PartialRow))
1. Specify Kudu `CreateTableOptions` (@javadoc[API](org.apache.kudu.client.CreateTableOptions))
1. Set up Alpakka's `KuduTableSettings` (@scaladoc[API](akka.stream.alpakka.kudu.KuduTableSettings)).


Scala
:   @@snip [snip](/kudu/src/test/scala/docs/scaladsl/KuduTableSpec.scala) { #configure }

Java
:   @@snip [snip](/kudu/src/test/java/docs/javadsl/KuduTableTest.java) { #configure }


## Writing to Kudu in a Flow

Scala
: @@snip [snip](/kudu/src/test/scala/docs/scaladsl/KuduTableSpec.scala) { #flow }

Java
: @@snip [snip](/kudu/src/test/java/docs/javadsl/KuduTableTest.java) { #flow }


## Writing to Kudu with a Sink

Scala
: @@snip [snip](/kudu/src/test/scala/docs/scaladsl/KuduTableSpec.scala) { #sink }

Java
: @@snip [snip](/kudu/src/test/java/docs/javadsl/KuduTableTest.java) { #sink }
