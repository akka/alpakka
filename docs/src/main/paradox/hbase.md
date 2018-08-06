# HBase

A flow and a composite sink to write element in [HBase](http://hbase.apache.org).

HBase is a column family NoSQL Database backed by HDFS.

### Reported issues

[Tagged issues at Github](https://github.com/akka/alpakka/labels/p%3Ahbase)

# Usage

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-hbase_$scalaBinaryVersion$
  version=$version$
}

Build a converter and a tableSetting.

Converter will map the domain object to list of HBase mutations (`Append`, `Delete`, `Increment`, `Put`).

Here some examples:

- A `Put` mutation:

scala
:   @@snip [snip](/hbase/src/test/scala/akka/stream/alpakka/hbase/scaladsl/HBaseStageSpec.scala) { #create-converter-put }

java
:   @@snip [snip](/hbase/src/test/java/akka/stream/alpakka/hbase/javadsl/HBaseStageTest.java) { #create-converter-put }

- An `Append` mutation:

scala
:   @@snip [snip](/hbase/src/test/scala/akka/stream/alpakka/hbase/scaladsl/HBaseStageSpec.scala) { #create-converter-append }

java
:   @@snip [snip](/hbase/src/test/java/akka/stream/alpakka/hbase/javadsl/HBaseStageTest.java) { #create-converter-append }

- A `Delete` mutation:

scala
:   @@snip [snip](/hbase/src/test/scala/akka/stream/alpakka/hbase/scaladsl/HBaseStageSpec.scala) { #create-converter-delete }

java
:   @@snip [snip](/hbase/src/test/java/akka/stream/alpakka/hbase/javadsl/HBaseStageTest.java) { #create-converter-delete }

- An `Increment` mutation:

scala
:   @@snip [snip](/hbase/src/test/scala/akka/stream/alpakka/hbase/scaladsl/HBaseStageSpec.scala) { #create-converter-increment }

java
:   @@snip [snip](/hbase/src/test/java/akka/stream/alpakka/hbase/javadsl/HBaseStageTest.java) { #create-converter-increment }


To ignore an object just return an empty `List`, this will have no effect on HBase.
You can also combine mutations to perform complex business logic:

scala
:   @@snip [snip](/hbase/src/test/scala/akka/stream/alpakka/hbase/scaladsl/HBaseStageSpec.scala) { #create-converter-complex }

java
:   @@snip [snip](/hbase/src/test/java/akka/stream/alpakka/hbase/javadsl/HBaseStageTest.java) { #create-converter-complex }

Remember that if you returns a list of mutations they will be applied in the same order.
The list of Mutations are not applied in an transaction, each mutation is independent.

Table will be created on demand.

scala
:   @@snip [snip](/hbase/src/test/scala/akka/stream/alpakka/hbase/scaladsl/HBaseStageSpec.scala) { #create-settings }

java
:   @@snip [snip](/hbase/src/test/java/akka/stream/alpakka/hbase/javadsl/HBaseStageTest.java) { #create-settings }

### Flow usage 

scala
: @@snip [snip](/hbase/src/test/scala/akka/stream/alpakka/hbase/scaladsl/HBaseStageSpec.scala) { #flow }

java
: @@snip [snip](/hbase/src/test/java/akka/stream/alpakka/hbase/javadsl/HBaseStageTest.java) { #flow }


### Sink usage

scala
: @@snip [snip](/hbase/src/test/scala/akka/stream/alpakka/hbase/scaladsl/HBaseStageSpec.scala) { #sink }

java
: @@snip [snip](/hbase/src/test/java/akka/stream/alpakka/hbase/javadsl/HBaseStageTest.java) { #sink }

## HBase basic command:

```
$HBASE_HOME/bin/start-hbase.sh

$HBASE_HOME/bin/ shell

```

From the hbase shell:

```
list //list table
scan "person" // select * from person
disable "person" //Disable table "person", before drop
drop "person" 
```
