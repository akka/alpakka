# Lightweight "Change Data Capture" for PostgreSQL

This provides an Akka Stream Source that can stream changes from a PostgreSQL database. Here, by
"change", we mean database events such as: RowDeleted, RowInserted, RowUpdated.

## How It Works

This Akka Stream Source makes use of the "logical decoding" feature of PostgreSQL (available since PostgreSQL 9.4).
It uses the "test_decoding" plugin that comes pre-packaged with PostgreSQL. Enabling a "logical decoding" slot
in PostgreSQL has minimal performance overhead and requires very little configuration.

## Limitations

* It cannot capture events regarding changes to tables' structure (i.e. column removed, table dropped, table
index added etc.).
* When a row update is captured, the previous version of the row is lost / not available.
* It doesn't work with older version of PostgreSQL (i.e < 9.4).

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-postgresql-cdc_$scalaBinaryVersion$
  version=$version$
}

Note that the PostgreSQL JDBC driver is not included in the JAR.

@@dependency [sbt,Maven,Gradle] {
  group=org.postgresql
  artifact=postgresql
  version=42.2.1
}

## Events Emitted

This Akka Streams Source emits elements of the type **ChangeSet** (transactionId: Long, changes: Set[Change])
where a **Change** can be one of the following:

* **RowInserted** (schemaName, tableName, fields: Set[Field])

* **RowUpdated** (schemaName, tableName, fields: Set[Field])

* **RowDeleted** (schemaName, tableName, fields: Set[Field])

A **Field** is defined as Field(columnName: String, columnType: String, value: String).

## Usage

### PostgreSQL: enable a "logical decoding" slot

The PostgreSQL documentation says that "before you can use logical decoding, you must set wal_level to logical and
max_replication_slots to at least 1". See [PostgreSQL documentation](https://www.postgresql.org/docs/9.4/static/logicaldecoding-example.html).

If you run your PostgreSQL on AWS RDS, you simply have to set the ```rds.logical_replication``` parameter to ```1``` in the option group
associated with your RDS instance. See [AWS News Blog](https://aws.amazon.com/blogs/aws/amazon-rds-for-postgresql-new-minor-versions-logical-replication-dms-and-more/).

To enable a "logical decoding" slot run the following query:

```
SELECT * FROM pg_create_logical_replication_slot('slot_name', 'test_decoding');
```

Note that any slot name is fine but this Akka Stream Source is only compatible (as of now)
with the 'test_decoding' plugin (which is the only logical decoding plugin that ships with PostgreSQL).

### Stream Changes, Map to Domain Events

You want to map these database change events (i.e. RowDeleted) to real domain events (i.e. UserDeregistered).

```scala

  // Define your domain event
  case class UserDeregistered(id: String)

  val connectionString = "jdbc:postgresql://localhost/test?user=fred&password=secret&ssl=true"
  val slotName = "slot_name"
  val settings = PostgreSQLChangeDataCaptureSettings(connectionString, slotName)

  PostgreSQLCapturer(settings)
    .map { c: ChangeSet =>
      c.changes.collect {
        case RowDeleted("public", "users", fields) => {
          val userId = fields.find(_.columnName == "user_id").map(_.value).getOrElse("unknown")
          UserDeregistered(userId)
        }
      }
    } // continue to a sink

```




