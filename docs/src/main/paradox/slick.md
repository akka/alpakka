# Slick (JDBC)

The Slick connector provides Scala and Java DSLs to create a `Source` to stream the results of a SQL database query and a `Flow`/`Sink` to perform SQL actions (like inserts, updates, and deletes) for each element in a stream. It is built on the [Slick](https://scala-slick.org/) library to interact with a long list of @extref[supported relational databases](slick:supported-databases.html).

@@project-info{ projectId="slick" }

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-slick_$scala.binary.version$
  version=$project.version$
  symbol2=AkkaVersion
  value2=$akka.version$
  group2=com.typesafe.akka
  artifact2=akka-stream_$scala.binary.version$
  version2=AkkaVersion
}

You will also need to add the JDBC driver(s) for the specific relational database(s) to your project. Most of those databases have drivers that are not available from public repositories so unfortunately some manual steps will probably be required. The Slick documentation has @extref[information on where to download the drivers](slick:supported-databases.html).

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="slick" }


## Initialization

As always, before we get started we will need an @apidoc[akka.actor.ActorSystem].

Scala
: @@snip [snip](/slick/src/test/scala/docs/scaladsl/SlickSpec.scala) { #init-mat }

Java
: @@snip [snip](/slick/src/test/java/docs/javadsl/SlickTest.java) { #init-mat }

You will also always need the following important imports:

Scala
: @@snip [snip](/slick/src/test/scala/docs/scaladsl/DocSnippets.scala) { #important-imports }

Java
: @@snip [snip](/slick/src/test/java/docs/javadsl/DocSnippetSource.java) { #important-imports }

The full examples for using the `Source`, `Sink`, and `Flow` (listed further down) also include all required imports.

## Starting a Database Session

All functionality provided by this connector requires the user to first create an instance of `SlickSession`, which is a thin wrapper around Slick's database connection management and database profile API.

If you are using Slick in your project, you can create a `SlickSession` instance by sharing the database configuration:

Scala
: @@snip [snip](/slick/src/test/scala/docs/scaladsl/SlickSpec.scala) { #init-db-config-session }

Otherwise, you can configure your database using [typesafe-config](https://github.com/lightbend/config) by adding a named configuration to your application.conf and then referring to that configuration when starting the session:

Scala
: @@snip [snip](/slick/src/test/scala/docs/scaladsl/SlickSpec.scala) { #init-session }

Java
: @@snip [snip](/slick/src/test/java/docs/javadsl/SlickTest.java) { #init-session }

Here is an example configuration for the H2 database, which is used for the unit tests of the Slick connector itself:

Configuration
: @@snip [snip](/slick/src/test/resources/application.conf) { #config-h2 }

You can specify multiple different database configurations, as long as you use unique names. These can then be loaded by fully qualified configuration name using the `SlickSession.forConfig()` method described above.

The Slick connector supports all the various ways Slick allows you to configure your JDBC database drivers, connection pools, etc., but we strongly recommend using the so-called @extref["DatabaseConfig"](slick:database.html#databaseconfig) method of configuration, which is the only method explicitly tested to work with the Slick connector.

Below are a few configuration examples for other databases. The Slick connector supports all @extref[databases supported by Slick](slick:supported-databases.html) (as of Slick 3.2.x)

Postgres
: @@snip [snip](/slick/src/test/resources/application.conf) { #config-postgres }

MySQL
: @@snip [snip](/slick/src/test/resources/application.conf) { #config-mysql }

DB2
: @@snip [snip](/slick/src/test/resources/application.conf) { #config-db2 }

Oracle
: @@snip [snip](/slick/src/test/resources/application.conf) { #config-oracle }

SQL Server
: @@snip [snip](/slick/src/test/resources/application.conf) { #config-sqlserver }

Of course these are just examples. Please visit the @extref[Slick documentation for `DatabaseConfig.fromConfig`](slick:api/index.html#slick.jdbc.JdbcBackend$DatabaseFactoryDef@forConfig(String,Config,Driver,ClassLoader%29:Database)) for the full list of things to configure.

You also have the option to create a SlickSession from Slick Database and Profile objects.

Scala
:  @@snip [snip](/slick/src/test/scala/docs/scaladsl/SlickSpec.scala) { #init-session-from-db-and-profile }

This can be useful if you need to share you configurations with code using Slick directly, or in cases where you first get the database information at runtime, as the Slicks Database class offer factory methods for that. This method is only available in the scaladsl, as Slick has no Java API and as such no easy way of creating a Database instance from Java.

## Closing a Database Session

Slick requires you to eventually close your database session to free up connection pool resources. You would usually do this when terminating the `ActorSystem`, by registering a termination handler like this:

Scala
: @@snip [snip](/slick/src/test/scala/docs/scaladsl/SlickSpec.scala) { #close-session }

Java
: @@snip [snip](/slick/src/test/java/docs/javadsl/SlickTest.java) { #close-session }

## Using a Slick Source

The Slick connector allows you to perform a SQL query and expose the resulting stream of results as an Akka Streams `Source[T]`. Where `T` is any type that can be constructed using a database row.

@@@ warning
Some database systems, such as PostgreSQL, require session parameters to be set in a certain way to support streaming without caching all data at once in memory on the client side, see [Slick documentation](https://scala-slick.org/doc/3.2.0/dbio.html#streaming).
```scala
// Example for PostgreSQL:
query.result.withStatementParameters(
  rsType = ResultSetType.ForwardOnly,
  rsConcurrency = ResultSetConcurrency.ReadOnly,
  fetchSize = 128, // not to be left at default value (0)
).transactionally
```
@@@

### Plain SQL queries

Both the Scala and Java DSLs support the use of @extref[plain SQL queries](slick:concepts.html#plain-sql-statements).

The Scala DSL expects you to use the special `sql"..."`, `sqlu"..."`, and `sqlt"..."` @extref[String interpolators provided by Slick](slick:sql.html#string-interpolation) to construct queries.

Unfortunately, String interpolation is a Scala language feature that cannot be directly translated to Java. This means that query strings in the Java DSL will need to be manually prepared using plain Java Strings (or a `StringBuilder`).

The following examples put it all together to perform a simple streaming query.

Scala
: @@snip [snip](/slick/src/test/scala/docs/scaladsl/DocSnippets.scala) { #source-example }

Java
: @@snip [snip](/slick/src/test/java/docs/javadsl/DocSnippetSource.java) { #source-example }


### Typed Queries

The Scala DSL also supports the use of @extref[Slick Scala queries](slick:concepts.html#scala-queries), which are more type-safe then their plain SQL equivalent. The code will look very similar to the plain SQL example.

Scala
: @@snip [snip](/slick/src/test/scala/docs/scaladsl/DocSnippets.scala) { #source-with-typed-query }


## Using a Slick Flow or Sink

If you want to take a stream of elements and turn them into side-effecting actions in a relational database, the Slick connector allows you to perform any DML or DDL statement using either a `Sink` or a `Flow`. This includes the typical `insert`/`update`/`delete` statements but also `create table`, `drop table`, etc. The unit tests have a couple of good examples of the latter usage.

The following example show the use of a Slick `Sink` to take a stream of elements and insert them into the database. There is an optional `parallelism` argument to specify how many concurrent streams will be sent to the database. The unit tests for the slick connector have example of performing parallel inserts.

Scala
: @@snip [snip](/slick/src/test/scala/docs/scaladsl/DocSnippets.scala) { #sink-example }

Java
: @@snip [snip](/slick/src/test/java/docs/javadsl/DocSnippetSink.java) { #sink-example }


## Flow

The Slick connector also exposes a `Flow` that has the exact same functionality as the `Sink` but it allows you to continue the stream for further processing. The return value of every executed statement, e.g. the element values is the fixed type `Int` denoting the number of updated/inserted/deleted rows.

Scala
: @@snip [snip](/slick/src/test/scala/docs/scaladsl/DocSnippets.scala) { #flow-example }

Java
: @@snip [snip](/slick/src/test/java/docs/javadsl/DocSnippetFlow.java) { #flow-example }


## Flow with pass-through

To have a different return type, use the `flowWithPassThrough` function.
E.g. when consuming Kafka messages, this allows you to maintain the kafka committable offset so the message can be committed in a next stage in the flow.

Scala
: @@snip [snip](/slick/src/test/scala/docs/scaladsl/DocSnippets.scala) { #flowWithPassThrough-example }

Java
: @@snip [snip](/slick/src/test/java/docs/javadsl/DocSnippetFlowWithPassThrough.java) { #flowWithPassThrough-example }
