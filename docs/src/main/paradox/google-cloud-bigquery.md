# Google Cloud BigQuery

The [Google Cloud BigQuery](https://cloud.google.com/bigquery/) connector provides connectivity to google BigQuery by running queries on large datasets and streaming the results.

### Reported issues

[Tagged issues at Github](https://github.com/akka/alpakka/labels/p%3Agoogle-cloud-bigquery)

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-google-cloud-bigquery_$scalaBinaryVersion$
  version=$version$
}

## Usage

Add the imports

Scala
: @@snip [snip](/google-cloud-bigquery/src/test/scala/docs/scaladsl/GoogleBigQuerySourceDoc.scala) { #imports }

Java
: @@snip [snip](/google-cloud-bigquery/src/test/java/docs/javadsl/GoogleBigQuerySourceDoc.java) { #imports }

Create the BigQuery configuration

Scala
: @@snip [snip](/google-cloud-bigquery/src/test/scala/docs/scaladsl/GoogleBigQuerySourceDoc.scala) { #init-config }

Java
: @@snip [snip](/google-cloud-bigquery/src/test/java/docs/javadsl/GoogleBigQuerySourceDoc.java) { #init-config }


You can use the connector in order to list information on the tables and their fields. 
The payload of the response from these requests is mapped to the models `QueryTableModel` and `Field`.
The results are mapped partially from the payload received.
In order to retrieve the full payload from these requests a custom parser has to be implemented. 
In case of error, empty response or API changes a custom parser has to be implemented.

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

If you want to use the built in paging implementation, or you have some specific needs you can call the raw api.
The next example shows how you can access [dryRun](https://cloud.google.com/bigquery/query-plan-explanation) data with the raw api and helpers.

Scala
: @@snip [snip](/google-cloud-bigquery/src/test/scala/docs/scaladsl/GoogleBigQuerySourceDoc.scala) { #dry-run }

Java
: @@snip [snip](/google-cloud-bigquery/src/test/java/docs/javadsl/GoogleBigQuerySourceDoc.java) { #dry-run }

### Config

The configuration will contain the session (which includes your service-token). 

If you create multiple requests to the same source (likely to happen) you should create a single `BigQueryConfig` instance and reuse it.

If you call multiple bigquery sources (not likely to happen) it is worth to cache the configs, so you can save a lot of unneeded authorization requests.

### Cancel on timeout

All of the provided functionality can fire a callback when the **downstream** signals a stop.
This is useful if you want to implement some timeout in the downstream, and try to lower your costs with stopping the longrunning jobs.
(Google doesn't provide any insurance about cost reduction, but at least we could try. [Read this for more information.](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/cancel))

Scala
: You can use the build in @scala[@scaladoc[BigQueryCallbacks](akka.stream.alpakka.googlecloud.bigquery.scaladsl.BigQueryCallbacks$).]

Java
: You can use the build in @java[@scaladoc[BigQueryCallbacks](akka.stream.alpakka.googlecloud.bigquery.javadsl.BigQueryCallbacks$).]

### Parsers

The parser function is a `JsObject => Try[T]` function. 
This is needed because there is a possibility, the response not to contain any data. In this case we need to retry the request with some delay.
Your parser function needs to be bulletproof and the code in the examples represents the happy path.
In case of `Failure` ; your stream will be polling forever!

## Running an End to End test case

You might want to run an End to End test case 

See @github:[BigQueryEndToEndSpec](../../../../google-cloud-bigquery/src/test/scala/akka/stream/alpakka/googlecloud/bigquery/e2e/BigQueryEndToEndSpec.scala).
To run this example using an actual GCP project you will need to configure a project, create/init tables in google-bigquery and provide a service account.
