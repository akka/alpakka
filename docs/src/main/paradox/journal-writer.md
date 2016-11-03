# Akka Persistence Journal Writer Connector

The Akka Persistence Journal Writer connector consists of an akka-streams Flow and Sink that makes it possible to write EventEnvelope , Seq[EventEnvelope], EventEnvelope2 or Seq[EventEnvelope2] to any akka-persistence jounal. It does this by sending messages directly to the journal plugin itself.

## Artifacts

sbt
:   @@@vars
    ```scala
    libraryDependencies += "com.typesafe.akka" %% "akka-stream-alpakka-journal-writer" % "$version$"
    ```
    @@@

Maven
:   @@@vars
    ```xml
    <dependency>
      <groupId>com.typesafe.akka</groupId>
      <artifactId>akka-stream-alpakka-journal-writer_$scala.binaryVersion$</artifactId>
      <version>$version$</version>
    </dependency>
    ```
    @@@

Gradle
:   @@@vars
    ```gradle
    dependencies {
      compile group: "com.typesafe.akka", name: "akka-stream-alpakka-journal-writer_$scala.binaryVersion$", version: "$version$"
    }
    ```
    @@@

## Basic Usage

The akka-persistence-journal-writer lets you write events to any journal. It accepts only two types of messages:

akka.persistence.query.EventEnvelope
akka.persistence.query.EventEnvelope2
Of course, you can send immutable.Seq[EventEnvelope] or immutable.Seq[EventEnvelope2] of those too for bulk loading.

The basic use case would be loading one event store into another. In this example we will be loading events from the inmemory-journal, using akka-persistence-query and loading the events into the level-db journal:

Scala
:   ```
import akka.stream.scaladsl._
import akka.persistence.query._
import akka.persistence.query.scaladsl._

val inMemoryReadJournal = PersistenceQuery(system).readJournalFor("inmemory-read-journal")
 .asInstanceOf[ReadJournal with CurrentPersistenceIdsQuery with CurrentEventsByPersistenceIdQuery]

val result: Future[Done] =
 inMemoryReadJournal.currentPersistenceIds().flatMapConcat { pid =>
  inMemoryReadJournal.currentEventsByPersistenceId(pid, 0, Long.MaxValue)
 }.grouped(100).runWith(JournalWriter.sink("akka.persistence.journal.leveldb"))
```

The fragment above reads all events from all persistenceIds from the inmemory-journal using akka-persistence-query and writes them directly into an empty level-db journal. Of course, any journal will work.

## Converting serialization strategy

Some akka-persistence-query compatible plugins support using the event-adapters from the write-plugin, therefor converting the journal's data-model to the application-model when querying the journal. Say for example that you have a journal that uses the Java-serialization strategy to store events and you would like to convert all those event using another serialization strategy, say Protobuf, then you can just configure some event adapters on the other write-plugin, say the level-db plugin and just load events from the in-memory plugin and store them into the level-db plugin. Akka-persistence will do all the work for you because you have configured the event-adapters on the plugin to do the serialization.

The solution is the configuration of the event-adapters on the write-plugins like eg:

```
inmemory-journal {
  event-adapters {
    adapter-a = "akka.stream.alpakka.persistence.writer.EventAdapterA"
  }
  event-adapter-bindings {
    "akka.stream.alpakka.persistence.writer.MyMessage" = adapter-a
  }
}

inmemory-read-journal {
  write-plugin = "inmemory-journal"
}

akka.persistence.journal.leveldb {
  dir = "target/journal"
  event-adapters {
    adapter-b = "akka.stream.alpakka.persistence.writer.EventAdapterB"
  }
  event-adapter-bindings {
    "akka.stream.alpakka.persistence.writer.MyMessage" = adapter-b
  }
}

akka.persistence.query.journal.leveldb {
  write-plugin = "akka.persistence.journal.leveldb"
}
```

In the example above, all events will be written to the in-memory journal using the event-adapter AdapterA. The inmemory-read-journal has been configured to use the event-adapters as configured from the inmemory-journal so when reading events using akka-persistence-query it should return application-domain events and not the java-serialized byte arrays.

When asking the akka-persistence-journal-writer (JournalWriter) to write events to a write plugin with a certain journalPluginId, eg. the level-db plugin, that plugin has been configured with certain event-adapters. Imagine that those event adapters will convert application-domain events to Protobuf types, then the protobuf serializer will serialize events to byte arrays and store those events in the level-db journal.

Of course, all events in the inmemory-journal will stay untouched. All events in the level-db journal will be protobuf-encoded.

## Bulk loading events

Say for a moment, that you have some business level entity with a stable identifier, and you have a lot of those. But those entities do not yet exist. They will exist only when they are created in the journal; ie. they have an initial state and a persistenceId of course.

Also imagine that the initial state is in some very large CSV file or JSON file that has been provided to you by an Apache Spark job for example.

You _could_ do the following:

- read each record,
- convert to an event,
- send events to shard,
- persistent actor will be recovered,
- event will be stored,
- persistent actor will be passified,
- actor will be unloaded from memory.
- This is in the book...

You could also do the following:

- read each record with a FileIO source
- convert the CSV to a the known event, wrap into a Seq[EventEnvelope]
- directly store these events in the journal
- This will have no persistent-actor life cycle overhead and will be much faster.

Its only applicable in some use cases of course.

### Running the example code

The code in this guide is part of runnable tests of this project. You are welcome to edit the code and run it in sbt.

Scala
:   ```
    sbt
    > journalWriter/test
    ```

Java
:   ```
    sbt
    > journalWriter/test
    ```
