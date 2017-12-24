# Slick (JDBC) Connector

The Slick connector provides Scala and Java DSLs to create a `Source` to stream the results of a SQL database query and a `Flow`/`Sink` to perform SQL actions (like inserts, updates, and deletes) for each element in a stream. It is built on the [Slick](http://slick.lightbend.com/) library to interact with a long list of [supported relational databases](http://slick.lightbend.com/doc/3.2.1/supported-databases.html).

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-slick_$scalaBinaryVersion$
  version=$version$
}

You will also need to add the JDBC driver(s) for the specific relational database(s) to your project. Most of those database have drivers that are not available from public repositories so unfortunately some manual steps will probably be required. The Slick documentation has [information on where to download the drivers](http://slick.lightbend.com/doc/3.2.1/supported-databases.html).

## Usage

As always, before we get started we will need an @scaladoc[ActorSystem](akka.actor.ActorSystem) and a @scaladoc[Materializer](akka.stream.Materializer).

Scala
: @@snip ($alpakka$/slick/src/test/scala/akka/stream/alpakka/slick/scaladsl/SlickSpec.scala) { #init-mat }

Java
: @@snip ($alpakka$/slick/src/test/java/akka/stream/alpakka/slick/javadsl/SlickTest.java) { #init-mat }

You will also always need the following important imports:

Scala
: @@snip ($alpakka$/slick/src/test/scala/akka/stream/alpakka/slick/scaladsl/DocSnippets.scala) { #important-imports }

Java
: @@snip ($alpakka$/slick/src/test/java/akka/stream/alpakka/slick/javadsl/DocSnippetSource.java) { #important-imports }

The full examples for using the `Source`, `Sink`, and `Flow` (listed further down) also include all required imports.

### Starting a Database Session

All functionality provided by this connector requires the user to first create an instance of `SlickSession`, which is a thin wrapper around Slick's database connection management and database profile API.

Scala
: @@snip ($alpakka$/slick/src/test/scala/akka/stream/alpakka/slick/scaladsl/SlickSpec.scala) { #init-session }

Java
: @@snip ($alpakka$/slick/src/test/java/akka/stream/alpakka/slick/javadsl/SlickTest.java) { #init-session }

As you can see, this requires you to configure your database using [typesafe-config](https://github.com/typesafehub/config) by adding a named configuration to your application.conf and then referring to that configuration when starting the session.

Here is an example configuration for the H2 database, which is used for the unit tests of the Slick connector itself:

Configuration
: @@snip ($alpakka$/slick/src/test/resources/application.conf) { #config-h2 }

You can specify multiple different database configurations, as long as you use unique names. These can then be loaded by fully qualified configuration name using the `SlickSession.forConfig()` method described above.

The Slick connector supports all the various ways Slick allows you to configure your JDBC database drivers, connection pools, etc., but we strongly recommend using the so-called ["DatabaseConfig"](http://slick.lightbend.com/doc/3.2.1/database.html#databaseconfig) method of configuration, which is the only method explicitly tested to work with the Slick connector.

Below are a few configuration examples for other databases. The Slick connector supports all [databases supported by Slick](http://slick.lightbend.com/doc/3.2.1/supported-databases.html) (as of Slick 3.2.x)

Postgres
: @@snip ($alpakka$/slick/src/test/resources/application.conf) { #config-postgres }

MySQL
: @@snip ($alpakka$/slick/src/test/resources/application.conf) { #config-mysql }

DB2
: @@snip ($alpakka$/slick/src/test/resources/application.conf) { #config-db2 }

Oracle
: @@snip ($alpakka$/slick/src/test/resources/application.conf) { #config-oracle }

SQL Server
: @@snip ($alpakka$/slick/src/test/resources/application.conf) { #config-sqlserver }

Of course these are just examples. Please visit the [Slick documentation for `DatabaseConfig.fromConfig`][jdbcbackend-api] for the full list of things to configure.

### Closing a Database Session
Slick requires you to eventually close your database session to free up connection pool resources. You would usually do this when terminating the `ActorSystem`, by registering a termination handler like this:

Scala
: @@snip ($alpakka$/slick/src/test/scala/akka/stream/alpakka/slick/scaladsl/SlickSpec.scala) { #close-session }

Java
: @@snip ($alpakka$/slick/src/test/java/akka/stream/alpakka/slick/javadsl/SlickTest.java) { #close-session }

### Using a Slick Source
The Slick connector allows you to perform a SQL query and expose the resulting stream of results as an Akka Streams `Source[T]`. Where `T` is any type that can be constructed using a database row.

#### Plain SQL queries
Both the Scala and Java DSLs support the use of [plain SQL queries](http://slick.lightbend.com/doc/3.2.1/concepts.html#plain-sql-statements).

The Scala DSL expects you to use the special `sql"..."`, `sqlu"..."`, and `sqlt"..."` [String interpolators provided by Slick](http://slick.lightbend.com/doc/3.2.1/sql.html#string-interpolation) to construct queries.

Unfortunately, String interpolation is a Scala language feature that cannot be directly translated to Java. This means that query strings in the Java DSL will need to be manually prepared using plain Java Strings (or a `StringBuilder`).

The following examples put it all together to perform a simple streaming query. The full source code for these examples can be found together with the unit tests of the Slick connector [on Github](https://github.com/akka/alpakka/tree/master/slick/src/test).

Scala
: @@snip ($alpakka$/slick/src/test/scala/akka/stream/alpakka/slick/scaladsl/DocSnippets.scala) { #source-example }

Java
: @@snip ($alpakka$/slick/src/test/java/akka/stream/alpakka/slick/javadsl/DocSnippetSource.java) { #source-example }


#### Typed Queries
The Scala DSL also supports the use of [Slick Scala queries](http://slick.lightbend.com/doc/3.2.1/concepts.html#scala-queries), which are more type-safe then their plain SQL equivalent. The code will look very similar to the plain SQL example.

Scala
: @@snip ($alpakka$/slick/src/test/scala/akka/stream/alpakka/slick/scaladsl/DocSnippets.scala) { #source-with-typed-query }


### Using a Slick Flow or Sink
If you want to take stream of elements and turn them into side-effecting actions in a relational database, the Slick connector allows you to perform any DML or DDL statement using either a `Sink` or a `Flow`. This includes the typical `insert`/`update`/`delete` statements but also `create table`, `drop table`, etc. The unit tests have a couple of good examples of the latter usage.

The following example show the use of a Slick `Sink` to take a stream of elements and insert them into the database. There is an optional `parallelism` argument to specify how many concurrent streams will be sent to the database. The unit tests for the slick connector have example of performing parallel inserts.

Scala
: @@snip ($alpakka$/slick/src/test/scala/akka/stream/alpakka/slick/scaladsl/DocSnippets.scala) { #sink-example }

Java
: @@snip ($alpakka$/slick/src/test/java/akka/stream/alpakka/slick/javadsl/DocSnippetSink.java) { #sink-example }

For completeness, the Slick connector also exposes a `Flow` that has the exact same functionality as the `Sink` but it allows you to continue the stream for further processing. The return value of every executed statement, e.g. the element values is an `Int` denoting the number of updated/inserted/deleted rows.

Scala
: @@snip ($alpakka$/slick/src/test/scala/akka/stream/alpakka/slick/scaladsl/DocSnippets.scala) { #flow-example }

Java
: @@snip ($alpakka$/slick/src/test/java/akka/stream/alpakka/slick/javadsl/DocSnippetFlow.java) { #flow-example }

 [jdbcbackend-api]: http://slick.lightbend.com/doc/3.2.1/api/index.html#slick.jdbc.JdbcBackend$DatabaseFactoryDef@forConfig(String,Config,Driver,ClassLoader):Database
