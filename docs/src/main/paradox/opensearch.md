# Opensearch

The Alpakka Elasticsearch connector also provides Akka Streams integration for Opensearch.

For more information about Opensearch, please visit the [Opensearch documentation](https://opensearch.org/docs/latest).

@@project-info{ projectId="elasticsearch" }

## Artifacts

The Akka dependencies are available from Akka's library repository. To access them there, you need to configure the URL for this repository.

@@repository [sbt,Maven,Gradle] {
id="akka-repository"
name="Akka library repository"
url="https://repo.akka.io/maven"
}

Additionally, add the dependencies as below.

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-elasticsearch_$scala.binary.version$
  version=$project.version$
  symbol2=AkkaVersion
  value2=$akka.version$
  group2=com.typesafe.akka
  artifact2=akka-stream_$scala.binary.version$
  version2=AkkaVersion
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="elasticsearch" }


### Opensearch connection

The connection and credentials to authenticate with are configured with `OpensearchConnectionSettings`.

Scala
: @@snip [snip](/elasticsearch/src/test/scala/docs/scaladsl/OpensearchConnectorBehaviour.scala) { #connection-settings }

Java
: @@snip [snip](/elasticsearch/src/test/java/docs/javadsl/OpensearchParameterizedTest.java) { #connection-settings }


| Parameter           | Default | Description                                                         |
| --------------------| ------- | ------------------------------------------------------------------- |
| baseUrl             | Empty   | The base URL of Opensearch. Should not include a trailing slash. |
| username            | None    | The username to authenticate with                                   |
| password            | None    | The password to authenticate with                                   |
| headers             | None    | List of headers that should be sent with the http request.          |
| connectionContext   | None    | The connectionContext that will be used with the http request. This can be used for TLS Auth instead of basic auth (username/password) by setting the SSLContext within the connectionContext.  |

## Opensearch parameters

Any API method that allows reading from and writing to Opensearch takes an instance of @apidoc[ElasticsearchParams$].

`ElasticsearchParams` has be constructed based on the Opensearch API version that you're targeting: 

Scala
: @@snip [snip](/elasticsearch/src/test/scala/docs/scaladsl/OpensearchConnectorBehaviour.scala) { #opensearch-params }

Java
: @@snip [snip](/elasticsearch/src/test/java/docs/javadsl/OpensearchParameterizedTest.java) { #opensearch-params }

## Opensearch as Source and Sink

You can stream messages from or to Opensearch using the
@apidoc[ElasticsearchSource$], @apidoc[ElasticsearchFlow$] or the @apidoc[ElasticsearchSink$].


Scala
: @@snip [snip](/elasticsearch/src/test/scala/docs/scaladsl/ElasticsearchSpecUtils.scala) { #define-class }

Java
: @@snip [snip](/elasticsearch/src/test/java/docs/javadsl/ElasticsearchTestBase.java) { #define-class }

### With typed source

Use `ElasticsearchSource.typed` and `ElasticsearchSink.create` to create source and sink.
@scala[The data is converted to and from JSON by Spray JSON.]
@java[The data is converted to and from JSON by Jackson's ObjectMapper.]

Scala
: @@snip [snip](/elasticsearch/src/test/scala/docs/scaladsl/OpensearchV1Spec.scala) { #run-typed }

Java
: @@snip [snip](/elasticsearch/src/test/java/docs/javadsl/OpensearchV1Test.java) { #run-typed }

### With JSON source

Use `ElasticsearchSource.create` and `ElasticsearchSink.create` to create source and sink.

Scala
: @@snip [snip](/elasticsearch/src/test/scala/docs/scaladsl/OpensearchV1Spec.scala) { #run-jsobject }

Java
: @@snip [snip](/elasticsearch/src/test/java/docs/javadsl/OpensearchV1Test.java) { #run-jsobject }


### Writing to Opensearch

In the above examples, `WriteMessage` is used as the input to `ElasticsearchSink` and `ElasticsearchFlow`. This means requesting `index` operation to Opensearch. It's possible to request other operations using following message types:

| Message factory           | Description                                                                                          |
| ---------------------- | ---------------------------------------------------------------------------------------------------- |
| WriteMessage.createIndexMessage   | Create a new document. If `id` is specified and it already exists, replace the document and increment its version.   |
| WriteMessage.createCreateMessage  | Create a new document. If `id` already exists, the `WriteResult` will contain an error.                  |
| WriteMessage.createUpdateMessage  | Update an existing document. If there is no document with the specified `id`, do nothing.            |
| WriteMessage.createUpsertMessage  | Update an existing document. If there is no document with the specified `id`, create a new document. |
| WriteMessage.createDeleteMessage  | Delete an existing document. If there is no document with the specified `id`, do nothing.            |

Scala
: @@snip [snip](/elasticsearch/src/test/scala/docs/scaladsl/OpensearchV1Spec.scala) { #multiple-operations }

Java
: @@snip [snip](/elasticsearch/src/test/java/docs/javadsl/OpensearchV1Test.java) { #multiple-operations }

### Source configuration

We can configure the source by `OpensearchSourceSettings`.

Scala
: @@snip [snip](/elasticsearch/src/test/scala/docs/scaladsl/OpensearchConnectorBehaviour.scala) { #source-settings }

Java
: @@snip [snip](/elasticsearch/src/test/java/docs/javadsl/OpensearchParameterizedTest.java) { #source-settings }

| Parameter              | Default        | Description                                                                                                              |
| ---------------------- | -------------- | ------------------------------------------------------------------------------------------------------------------------ |
| connection             |                | The connection details and credentials to authenticate against Opensearch. See `OpensearchConnectionSettings` |
| bufferSize             | 10             | `ElasticsearchSource` retrieves messages from Opensearch by scroll scan. This buffer size is used as the scroll size. | 
| includeDocumentVersion | false          | Tell Opensearch to return the documents `_version` property with the search results. See [Version](https://www.elastic.co/guide/en/elasticsearch/reference/7.10/search-request-body.html#request-body-search-version) and [Optimistic Concurrency Control](https://www.elastic.co/guide/en/elasticsearch/guide/current/optimistic-concurrency-control.html) to know about this property. |
| scrollDuration         | 5 min          | `ElasticsearchSource`  retrieves messages from Opensearch by scroll scan. This parameter is used as a scroll value. See [Time units](https://www.elastic.co/guide/en/elasticsearch/reference/7.10/common-options.html#time-units) for supported units.                |
| apiVersion             | V1             | Currently supports `V1` (see below) |

### Sink and flow configuration

Sinks and flows are configured with `OpensearchWriteSettings`.

Scala
: @@snip [snip](/elasticsearch/src/test/scala/docs/scaladsl/OpensearchConnectorBehaviour.scala) { #sink-settings }

Java
: @@snip [snip](/elasticsearch/src/test/java/docs/javadsl/OpensearchParameterizedTest.java) { #sink-settings }

| Parameter           | Default    | Description                                                                                            |
| ------------------- | -------    | ------------------------------------------------------------------------------------------------------ |
| connection          |            | The connection details and credentials to authenticate against Opensearch. See `OpensearchConnectionSettings` |
| bufferSize          | 10         | Flow and Sink batch messages to bulk requests when back-pressure applies.                              |
| versionType         | None       | If set, `ElasticsearchSink` uses the chosen versionType to index documents. See [Version types](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-index_.html#_version_types) for accepted settings. |
| retryLogic          | No retries | See below |
| apiVersion          | V1         | Currently supports `V1` (see below) |
| allowExplicitIndex  | True       | When set to False, the index name will be included in the URL instead of on each document (see below) | 

#### Retry logic
A bulk request might fail partially for some reason. To retry failed writes to Opensearch, a `RetryLogic` can be specified. 

The provided implementations are:

* `RetryAtFixedRate`

| Parameter           | Description     |
|---------------------|-----------------|
| maxRetries          | The stage fails, if it gets this number of consecutive failures. | 
| retryInterval       | Failing writes are retried after this duration. |

* `RetryWithBackoff`

| Parameter           | Description     |
|---------------------|-----------------|
| maxRetries          | The stage fails, if it gets this number of consecutive failures. | 
| minBackoff          | Initial backoff for failing writes. |
| maxBackoff          | Maximum backoff for failing writes. |

In case of write failures the order of messages downstream is guaranteed to be preserved.

#### Supported API versions
To support reading and writing to multiple versions of Opensearch, an `OpensearchApiVersion` can be specified.

This will be used to:
1. transform the bulk request into a format understood by the corresponding Opensearch server.
2. determine whether to include the index type mapping in the API calls. See [removal of types](https://www.elastic.co/guide/en/elasticsearch/reference/current/removal-of-types.html)

Currently [`V1`](https://opensearch.org/docs/1.3/opensearch/rest-api/document-apis/bulk/) is supported specifically but this parameter does not need to match the server version exactly (for example, `V1` should also work with Opensearch 2.x).

### Allow explicit index

When using the `_bulk` API, Opensearch will reject requests that have an explicit index in the request body if explicit index names are not allowed. See [URL-based access control](https://www.elastic.co/guide/en/elasticsearch/reference/7.10/url-access-control.html)

## Opensearch as Flow

You can also build flow stages with @apidoc[ElasticsearchFlow$].
The API is similar to creating Sinks.

Scala
: @@snip [snip](/elasticsearch/src/test/scala/docs/scaladsl/OpensearchV1Spec.scala) { #run-flow }

Java
: @@snip [snip](/elasticsearch/src/test/java/docs/javadsl/OpensearchV1Test.java) { #run-flow }


### Storing documents from Strings

Opensearch requires the documents to be properly formatted JSON. If your data is available as JSON in Strings, you may use the pre-defined `StringMessageWriter` to avoid any conversions. For any other JSON technologies, implement a @scala[`MessageWriter[T]`]@java[`MessageWriter<T>`].

Scala
: @@snip [snip](/elasticsearch/src/test/scala/docs/scaladsl/OpensearchV1Spec.scala) { #string }

Java
: @@snip [snip](/elasticsearch/src/test/java/docs/javadsl/OpensearchV1Test.java) { #string }



### Passing data through ElasticsearchFlow

When streaming documents from Kafka, you might want to commit to Kafka **AFTER** the document has been written to Opensearch.

Scala
: @@snip [snip](/elasticsearch/src/test/scala/docs/scaladsl/OpensearchV1Spec.scala) { #kafka-example }

Java
: @@snip [snip](/elasticsearch/src/test/java/docs/javadsl/OpensearchV1Test.java) { #kafka-example }


### Specifying custom index-name for every document

When working with index-patterns using wildcards, you might need to specify a custom
index-name for each document:

Scala
: @@snip [snip](/elasticsearch/src/test/scala/docs/scaladsl/OpensearchV1Spec.scala) { #custom-index-name-example }

Java
: @@snip [snip](/elasticsearch/src/test/java/docs/javadsl/OpensearchParameterizedTest.java) { #custom-index-name-example }


### Specifying custom metadata for every document

In some cases you might want to specify custom metadata per document you are inserting, for example a `pipeline`, 
this can be done like so:

Scala
: @@snip [snip](/elasticsearch/src/test/scala/docs/scaladsl/OpensearchConnectorBehaviour.scala) { #custom-metadata-example }

Java
: @@snip [snip](/elasticsearch/src/test/java/docs/javadsl/OpensearchParameterizedTest.java) { #custom-metadata-example }

 
### More custom searching

The easiest way of using Opensearch-source, is to just specify the query-param. Sometimes you need more control,
like specifying which fields to return and so on. In such cases you can instead use 'searchParams' instead:

Scala
: @@snip [snip](/elasticsearch/src/test/scala/docs/scaladsl/OpensearchV1Spec.scala) { #custom-search-params }

Java
: @@snip [snip](/elasticsearch/src/test/java/docs/javadsl/OpensearchV1Test.java) { #custom-search-params }


#### Routing
Support for [custom routing](https://www.elastic.co/guide/en/elasticsearch/reference/7.10/mapping-routing-field.html) 
is available through the `routing` key. Add this key and the respective value in 'searchParams' map, to route your search directly to the shard that holds the document you are looking for and enjoy improved response times.

#### Sort
Support for sort is available through the `sort` key in `searchParams` map. If no sort is given, the source will use `sort=_doc` to maximize performance, as indicated by [Opensearch documentation](https://opensearch.org/docs/latest/opensearch/rest-api/scroll/).
