# Lightweight "Change Data Capture" for PostgreSQL

This provides an Akka Stream Source that can stream changes from a PostgreSQL database. Here, by
"change", we mean database events such as: RowDeleted(..), RowInserted(..), RowUpdated(..). A
typical practical use case  is to have a stream that continuously replicates data from PostgreSQL to ElasticSearch. But more generally,
this provides the tooling for implementing the [Strangler Application](https://www.martinfowler.com/bliki/StranglerApplication.html) /
[Event Interception](https://www.martinfowler.com/bliki/EventInterception.html) patterns that Martin Fowler popularized.

## How It Works

This Akka Stream Source makes use of the "logical decoding" feature of PostgreSQL (available since PostgreSQL 9.4).
It uses the `test_decoding` plugin that comes pre-packaged with PostgreSQL. Enabling a "logical decoding" slot
in PostgreSQL requires very little configuration and, generally, has minimal performance overhead. However, please consult
the PostgreSQL documentation to understand the performance implications: [the PostgreSQL documentation](https://www.postgresql.org/docs/10.4/static/logicaldecoding-example.html).

## Events Emitted

This Akka Streams Source emits elements of the type @scaladoc[ChangeSet](akka.stream.alpakka.postgresqlcdc.ChangeSet). A change set is a set of changes that share a
transaction id. A 'change' can be one of the following:

* @scaladoc[RowInserted](akka.stream.alpakka.postgresqlcdc.RowInserted)
    * schemaName: String
    * tableName: String
    * fields: A list of fields

* @scaladoc[RowUpdated](akka.stream.alpakka.postgresqlcdc.RowUpdated)
    * schemaName: String
    * tableName: String
    * fields: A list of fields
        * note: only the new version of the fields (by default)

* @scaladoc[RowDeleted](akka.stream.alpakka.postgresqlcdc.RowDeleted)
    * schemaName: String
    * tableName: String
    * fields: A list of fields

A @scaladoc[Field](akka.stream.alpakka.postgresqlcdc.Field) is defined as a class with 3 attributes of type String : columnName, columnType, value. The onus is on the user to turn the stringly-typed @scaladoc[Field](akka.stream.alpakka.postgresqlcdc.Field)
into something domain specific and more strongly typed.

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-postgresql-cdc_$scalaBinaryVersion$
  version=$version$
}

Note that the PostgreSQL JDBC driver is not included in the main JAR.

@@dependency [sbt,Maven,Gradle] {
  group=org.postgresql
  artifact=postgresql
  version=42.2.1
}

### Reported Issues
[Tagged issues at Github](https://github.com/akka/alpakka/labels/p%3Apostgresql-cdc)


## Usage

### PostgreSQL Configuration (Required)

According to the PostgreSQL documentation, before you can use logical decoding, you must set `wal_level` to `logical` and
`max_replication_slots` to at least 1. This can be as simple as:

```bash
echo "wal_level=logical" >> /etc/postgresql/9.4/main/postgresql.conf
echo "max_replication_slots=8" >> /etc/postgresql/9.4/main/postgresql.conf
```

If you use a cloud-based managed database service (e.g., AWS RDS), you usually don't have direct access to the `postgresql.conf` configuration file and you have to use
a configuration panel instead:

* On AWS RDS:
    * Use option groups: you simply have to set the ```rds.logical_replication``` parameter to ```1``` in the option group associated with your RDS instance. See [the AWS RDS documentation](https://aws.amazon.com/blogs/aws/amazon-rds-for-postgresql-new-minor-versions-logical-replication-dms-and-more/).

### Source Settings

We configure the source using @scaladoc[PostgreSQLInstance](akka.stream.alpakka.postgresqlcdc.PostgreSQLInstance) and @scaladoc[ChangeDateCaptureSettings](akka.stream.alpakka.postgresqlcdc.ChangeDataCaptureSettings):

@scaladoc[PostgreSQLInstance](akka.stream.alpakka.postgresqlcdc.PostgreSQLInstance):

|Setting                 | Meaning                           | Required|
| -----------------------|-----------------------------------|---------|
| JDBC connection string | JDBC connection string            | yes     |
| slot name              | name of logical replication slot  | yes     |

@scaladoc[ChangeDateCaptureSettings](akka.stream.alpakka.postgresqlcdc.ChangeDataCaptureSettings):

| Setting           | Meaning                                   | Default   |
| ------------------|-------------------------------------------|-----------|
| createSlotOnStart | create logical replication slot on start  | true      |
| tablesToIgnore    | a list of tables to ignore                | empty     |
| columnsToIgnore   | what columns to ignore per table          | empty     |
| mode              | choose between "Get" (at most once) or  "Peek" (at least once). If you choose "Peek", you'll need an ack sink or flow, to acknowledge consumption of the event  | Get |
| maxItems          | maximum number of "changes" to pull in one go | 128   |
| pollInterval      | duration between polls                    | 2 seconds |

Example source settings:

Scala
: @@snip ($alpakka$/postgresql-cdc/src/test/scala/akka/stream/alpakka/postgresqlcdc/TestScalaDsl.scala) { #ChangeDataCaptureSettings }

Java
: @@snip ($alpakka$/postgresql-cdc/src/test/java/akka/stream/alpakka/postgresqlcdc/TestJavaDsl.java) { #ChangeDataCaptureSettings }



### Code

Without further ado, a minimalist example:

Scala
: @@snip ($alpakka$/postgresql-cdc/src/test/scala/akka/stream/alpakka/postgresqlcdc/TestScalaDsl.scala) { #BasicExample }

Java
: @@snip ($alpakka$/postgresql-cdc/src/test/java/akka/stream/alpakka/postgresqlcdc/TestJavaDsl.java) { #BasicExample }



### Stream Changes, Map to Domain Events

You want to map these database change events (i.e. RowDeleted) to real domain events (i.e. UserDeregistered), aiming to adopt a Domain Driven Design approach.

Scala
: @@snip ($alpakka$/postgresql-cdc/src/test/scala/akka/stream/alpakka/postgresqlcdc/TestScalaDsl.scala) { #ProcessEventsExample }

Java
: @@snip ($alpakka$/postgresql-cdc/src/test/java/akka/stream/alpakka/postgresqlcdc/TestJavaDsl.java) { #ProcessEventsExample }



## Limitations

* It cannot capture events regarding changes to tables' structure (i.e. column removed, table dropped, table
index added, change of column type etc.).
* When a row update is captured, the previous version of the row is lost / not available.
    * Unless the `REPLICA IDENTITY` setting for the table is changed from the default.
* It doesn't work with older version of PostgreSQL (i.e < 9.4).