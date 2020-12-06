# Google Cloud BigQuery

The [Google Cloud BigQuery](https://cloud.google.com/bigquery/) connector provides connectivity to Google BigQuery by running queries on large datasets and streaming the results.

@@project-info{ projectId="google-cloud-bigquery" }

@@@warning { title="API may change" }

Alpakka Google Cloud BigQuery was added in Alpakka 2.0.2 in July 2020 and is marked as "API may change". Please try it out and suggest improvements. [Issue #2353](https://github.com/akka/alpakka/issues/2353)

@@@

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-google-cloud-bigquery_$scala.binary.version$
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

For the rawest representation there is a "csvStyle" source built-in.
This will return a header (field names), and the fields as a list of Strings.

Scala
: @@snip [snip](/google-cloud-bigquery/src/test/scala/docs/scaladsl/GoogleBigQuerySourceDoc.scala) { #csv-style }

Java
: @@snip [snip](/google-cloud-bigquery/src/test/java/docs/javadsl/GoogleBigQuerySourceDoc.java) { #csv-style }

There is a more sophisticated way to get data from a database.
@scala[If you want to get a stream of classes, you can create a case class that models your data.]
@java[If you want to get a stream of classes, you can add your converter function too.]

Scala
: @@snip [snip](/google-cloud-bigquery/src/test/scala/docs/scaladsl/GoogleBigQuerySourceDoc.scala) { #run-query }

Java
: @@snip [snip](/google-cloud-bigquery/src/test/java/docs/javadsl/GoogleBigQuerySourceDoc.java) { #run-query }

If you want to use the built in paging implementation, or you have some specific needs you can call the raw API.
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

You can use the built-in @apidoc[BigQueryCallbacks$].

### Parsers

The provided parser is implemented with the Spray JSON library.
@scala[To use, bring into scope the required implicits with `import akka.stream.alpakka.googlecloud.bigquery.scaladsl.SprayJsonSupport._` and provide either an implicit `spray.json.JsonFormat[T]` for `GoogleBigQuerySource[T].runQuery` or `spray.json.RootJsonFormat[T]` for `GoogleBigQuerySource[T].raw`.]
@java[To use, you must provide a `java.util.function.Function<spray.json.JsObject, scala.util.Try<T>>` function.]

When calling the `GoogleBigQuerySource.raw` method, your parser is passed the [entire response body](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/getQueryResults#response-body).
When calling the `GoogleBigQuerySource.runQuery` method, your parser is passed one-by-one each entry in the `rows` key of the response.
Note that these entries are represented as "a series of JSON f,v objects for indicating fields and values" ([reference documentation](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/getQueryResults#body.GetQueryResultsResponse.FIELDS.rows)).
@scala[To correctly generate a Spray `JsonFormat[T]` for your case class `T`, you must `import akka.stream.alpakka.googlecloud.bigquery.scaladsl.BigQueryJsonProtocol._` and use the provided `bigQueryJsonFormatN(T)` methods as a drop-in replacement for the usual `jsonFormatN(T)`. Furthermore, the order and presence of parameters in `T` is required to strictly match the query.]

The actual parsing implementation is fully customizable via the [unmarshalling API](https://doc.akka.io/docs/akka-http/current/common/unmarshalling.html) from Akka HTTP.
This lets you bring your own JSON library as an alternative to Spray.
Letting `J` be the type of the JSON representation (e.g., `JsValue` for Spray), you must provide @scala[implicits] @java[instances] for:

* @scala[`FromByteStringUnmarshaller[J]`] @java[`Unmarshaller<akka.util.ByteString, J>`]
* @scala[`Unmarshaller[J, ResponseJsonProtocol.Response]`] @java[`Unmarshaller<J, ResponseJsonProtocol.Response>`]
* @scala[`Unmarshaller[J, ResponseJsonProtocol.ResponseRows[T]]` for `GoogleBigQuerySource[T].runQuery` or `Unmarshaller[J, T]` for `GoogleBigQuerySource[T].raw`] @java[`Unmarshaller<J, ResponseJsonProtocol.ResponseRows<T>>` for `GoogleBigQuerySource.runQuery` or `Unmarshaller<J, T>` for `GoogleBigQuerySource.raw`]

The following example revisits the `User` query from above, this time with all parsing handled by @scala[[circe](https://circe.github.io/circe/)] @java[[Jackson](https://github.com/FasterXML/jackson)].

Scala
: @@snip [snip](/google-cloud-bigquery/src/test/scala/docs/scaladsl/GoogleBigQuerySourceCustomParserDoc.scala) { #custom-parser }

Java
: @@snip [snip](/google-cloud-bigquery/src/test/java/docs/javadsl/GoogleBigQuerySourceCustomParserDoc.java) { #custom-parser-imports #custom-parser }

## Running an End to End test case

You might want to run an End-to-End test case.

See @github:[BigQueryEndToEndSpec](../../../../google-cloud-bigquery/src/test/scala/akka/stream/alpakka/googlecloud/bigquery/e2e/BigQueryEndToEndSpec.scala).
To run this example using an actual GCP project you will need to configure a project, create/init tables in google-bigquery and provide a service account.
