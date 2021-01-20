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
