# Apache Geode

[Apache Geode](http://geode.apache.org) is a distributed datagrid (ex Gemfire).

This connector provides flow and a sink to put element in and source to retrieve element from geode.

Basically it can store data as key, value. Key and value must be serialized, more on this later.

### Reported issues

[Tagged issues at Github](https://github.com/akka/alpakka/labels/p%3Ageode)

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-geode_$scalaBinaryVersion$
  version=$version$
}

#Usage

##Connection

First of all you need to connect to the geode cache. In a client application, connection is handle by a
 @extref[ClientCache](geode:basic_config/the_cache/managing_a_client_cache.html). A single
 ClientCache per application is enough. ClientCache also holds a single PDXSerializer.

scala
: @@snip [snip](/geode/src/test/scala/docs/scaladsl/GeodeFlowSpec.scala) { #connection }

java
: @@snip [snip](/geode/src/test/java/docs/javadsl/GeodeBaseTestCase.java) { #connection }

Apache Geode supports continuous queries. Continuous query relies on server event, thus reactive geode needs to listen to
 those event. This behaviour, as it consumes more resources is isolated in a scala trait and/or an specialized java class.

scala
: @@snip [snip](/geode/src/test/scala/docs/scaladsl/GeodeContinuousSourceSpec.scala) { #connection-with-pool }

java
: @@snip [snip](/geode/src/test/java/docs/javadsl/GeodeBaseTestCase.java) { #connection-with-pool }

##Region

Define a @extref[region](geode:/basic_config/data_regions/chapter_overview.html) setting to
describe how to access region and the key extraction function.

scala
: @@snip [snip](/geode/src/test/scala/docs/scaladsl/GeodeBaseSpec.scala) { #region }

java
: @@snip [snip](/geode/src/test/java/docs/javadsl/GeodeBaseTestCase.java) { #region }


###Serialization

Object must be serialized to flow in a geode region.

* opaque format (eq json/xml)
* java serialisation
* pdx geode format

PDX format is the only one supported.

PDXEncoder support many options, see @extref[gemfire_pdx_serialization.html](geode:/developing/data_serialization/gemfire_pdx_serialization.html)

PdxSerializer must be provided to geode when reading or writing to a region.

scala
:   @@snip [snip](/geode/src/test/scala/docs/scaladsl/PersonPdxSerializer.scala) { #person-pdx-serializer }

java
:   @@snip [snip](/geode/src/test/java/docs/javadsl/PersonPdxSerializer.java) { #person-pdx-serializer }



This project provides a generic solution for scala user based on [shapeless](https://github.com/milessabin/shapeless), then case classe serializer if not provided will be generated compile time.
Java user will need to write by hand their custom serializer.


Runtime reflection is also an option see @extref[auto_serialization.html](geode:/developing/data_serialization/auto_serialization.html).

###Flow usage

This sample stores (case) classes in Geode.

scala
: @@snip [snip](/geode/src/test/scala/docs/scaladsl/GeodeFlowSpec.scala) { #flow }

java
: @@snip [snip](/geode/src/test/java/docs/javadsl/GeodeFlowTestCase.java) { #flow }


###Sink usage

scala
: @@snip [snip](/geode/src/test/scala/docs/scaladsl/GeodeSinkSpec.scala) { #sink }

java
: @@snip [snip](/geode/src/test/java/docs/javadsl/GeodeSinkTestCase.java) { #sink }


###Source usage

####Simple query

Apache Geode support simple queries.

scala
: @@snip [snip](/geode/src/test/scala/docs/scaladsl/GeodeFiniteSourceSpec.scala) { #query }

java
: @@snip [snip](/geode/src/test/java/docs/javadsl/GeodeFiniteSourceTestCase.java) { #query }


####Continuous query


scala
: @@snip [snip](/geode/src/test/scala/docs/scaladsl/GeodeContinuousSourceSpec.scala) { #continuousQuery }

java
: @@snip [snip](/geode/src/test/java/docs/javadsl/GeodeContinuousSourceTestCase.java) { #continuousQuery }


##Geode basic command:

Assuming Apache geode is installed:

```
gfsh
```

From the geode shell:

```
start locator --name=locator
configure pdx --read-serialized=true
start server --name=server

create region --name=animals --type=PARTITION_REDUNDANT --redundant-copies=2
create region --name=persons --type=PARTITION_REDUNDANT --redundant-copies=2

```

###Run the test

Integration test are run against localhost geode, but IT_GEODE_HOSTNAME environment variable can change this:

> Test code requires Geode running in the background. You can start it quickly using docker:
>
> `docker-compose up geode`

Scala
:   ```
    sbt
    > geode/testOnly *Spec
    ```

Java
:   ```
    sbt
    > geode/testOnly *Test
    ```
