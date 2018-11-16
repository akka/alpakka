# Google Cloud BigQuery

The [Google Cloud BigQuery](https://cloud.google.com/bigquery/) connector provides a way to connect to google bigquery, 
run querys on large datasets and get the results streamed.

### Reported issues

[Tagged issues at Github](https://github.com/akka/alpakka/labels/p%3Agoogle-cloud-bigquery)

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-google-cloud-bigquery_$scalaBinaryVersion$
  version=$version$
}

## Usage

Possibly needed imports for the following codes

Scala
: @@snip [snip](/google-cloud-bigquery/src/test/scala/docs/scaladsl/GoogleBigQuerySourceDoc.scala) { #imports }

Java
: @@snip [snip](/google-cloud-bigquery/src/test/java/docs/javadsl/GoogleBigQuerySourceDoc.java) { #imports }

At the beginning you will need a config to work with. 

Scala
: @@snip [snip](/google-cloud-bigquery/src/test/scala/docs/scaladsl/GoogleBigQuerySourceDoc.scala) { #init-config }

Java
: @@snip [snip](/google-cloud-bigquery/src/test/java/docs/javadsl/GoogleBigQuerySourceDoc.java) { #init-config }


The connector has some fire and forget style API for meta data requests. 
(These give back basic information about the tables and the fields. If you need more information, sadly you need to write some parsers yourself (see below).)

Scala
: @@snip [snip](/google-cloud-bigquery/src/test/scala/docs/scaladsl/GoogleBigQuerySourceDoc.scala) { #list-tables-and-fields }

Java
: @@snip [snip](/google-cloud-bigquery/src/test/java/docs/javadsl/GoogleBigQuerySourceDoc.java) { #list-tables-and-fields }

For the rawest representation there is a "csvStyle" source built in. 
This will return a header (field names), and the fields as a list of Strings.

Scala
: @@snip [snip](/google-cloud-bigquery/src/test/scala/docs/scaladsl/GoogleBigQuerySourceDoc.scala) { #csv-style }

Java
: @@snip [snip](/google-cloud-bigquery/src/test/java/docs/javadsl/GoogleBigQuerySourceDoc.java) { #csv-style }

There is a more sophisticated way to get data from a database.
If you want to get a stream of classes, you can add your converter function too.

Scala
: @@snip [snip](/google-cloud-bigquery/src/test/scala/docs/scaladsl/GoogleBigQuerySourceDoc.scala) { #run-query }

Java
: @@snip [snip](/google-cloud-bigquery/src/test/java/docs/javadsl/GoogleBigQuerySourceDoc.java) { #run-query }

If you just want to use the built in paging implementation, or you have some specific need you can call the raw api.
The next example shows how you can access some [dryRun](https://cloud.google.com/bigquery/query-plan-explanation) data with the raw api and helpers.

Scala
: @@snip [snip](/google-cloud-bigquery/src/test/scala/docs/scaladsl/GoogleBigQuerySourceDoc.scala) { #dry-run }

Java
: @@snip [snip](/google-cloud-bigquery/src/test/java/docs/javadsl/GoogleBigQuerySourceDoc.java) { #dry-run }

### Config

The config will handle your session, and your service-token. (See the creation code above.)

If you create multiple requests to the same source (likely to happen) you should create it once and try to reuse it.
If you call multiple bigquery sources (not likely to happen) it is worth to cache the configs, so you can save a lot of unneeded authorization requests.

### Cancel on timeout

All off the provided functionality can fire a callback when the **downstream** signals a stop.
This is handful if you want to implement some timeout in the downstream, and try to lower your costs with stopping the longrunning jobs.
(Google doesn't provide any insurance about cost reduction, but at least we could try. [Read this for more information.](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/cancel))

Scala
: You can use the build in @scaladoc[BigQueryCallbacks](akka.stream.alpakka.google.cloud.bigquery.scaladsl.BigQueryCallbacks)

Java
: You can use the build in @scaladoc[BigQueryCallbacks](akka.stream.alpakka.google.cloud.bigquery.javadsl.BigQueryCallbacks)

### Parsers

The parser function is a `JsObject => Option[T]` function. 
This is needed, because there is a possibility that the response not contains any data, and in that case we need to retry the request with some delay.
Your parser function needs to be bulletproof and the codes in the examples are not the good practices for this.
If you returns `None` in every error case; your stream will be polling forever!

## Running the examples

To run the example code you will need to configure a project, create/init tables in google-bigquery and provide your own credentials.
