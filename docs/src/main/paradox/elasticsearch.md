# Elasticsearch

The Alpakka Elasticsearch connector provides Akka Streams integration for Elasticsearch.

For more information about Elasticsearch, please visit the [Elasticsearch documentation](https://www.elastic.co/guide/index.html).

@@project-info{ projectId="elasticsearch" }

## Artifacts

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


## Set up REST client

Sources, Flows and Sinks provided by this connector need a prepared `org.elasticsearch.client.RestClient` to
access to Elasticsearch.

Scala
: @@snip [snip](/elasticsearch/src/test/scala/docs/scaladsl/ElasticsearchSpec.scala) { #init-client }

Java
: @@snip [snip](/elasticsearch/src/test/java/docs/javadsl/ElasticsearchTest.java) { #init-client }

## Elasticsearch as Source and Sink

Now we can stream messages from or to Elasticsearch by providing the `RestClient` to the
@apidoc[ElasticsearchSource$], @apidoc[ElasticsearchFlow$] or the @apidoc[ElasticsearchSink$].


Scala
: @@snip [snip](/elasticsearch/src/test/scala/docs/scaladsl/ElasticsearchSpec.scala) { #define-class }

Java
: @@snip [snip](/elasticsearch/src/test/java/docs/javadsl/ElasticsearchTest.java) { #define-class }

### With typed source

Use `ElasticsearchSource.typed` and `ElasticsearchSink.create` to create source and sink.
@scala[The data is converted to and from JSON by Spray JSON.]
@java[The data is converted to and from JSON by Jackson's ObjectMapper.]

Scala
: @@snip [snip](/elasticsearch/src/test/scala/docs/scaladsl/ElasticsearchSpec.scala) { #run-typed }

Java
: @@snip [snip](/elasticsearch/src/test/java/docs/javadsl/ElasticsearchTest.java) { #run-typed }

### With JSON source

Use `ElasticsearchSource.create` and `ElasticsearchSink.create` to create source and sink.

Scala
: @@snip [snip](/elasticsearch/src/test/scala/docs/scaladsl/ElasticsearchSpec.scala) { #run-jsobject }

Java
: @@snip [snip](/elasticsearch/src/test/java/docs/javadsl/ElasticsearchTest.java) { #run-jsobject }


### Writing to Elasticsearch

In the above examples, `WriteMessage` is used as the input to `ElasticsearchSink` and `ElasticsearchFlow`. This means requesting `index` operation to Elasticsearch. It's possible to request other operations using following message types:

| Message factory           | Description                                                                                          |
| ---------------------- | ---------------------------------------------------------------------------------------------------- |
| WriteMessage.createIndexMessage   | Create a new document. If `id` is specified and it already exists, do nothing.                       |
| WriteMessage.createCreateMessage  | Create a new document. If `id` already exists, the `WriteResult` will contain an error.                  |
| WriteMessage.createUpdateMessage  | Update an existing document. If there is no document with the specified `id`, do nothing.            |
| WriteMessage.createUpsertMessage  | Update an existing document. If there is no document with the specified `id`, create a new document. |
| WriteMessage.createDeleteMessage  | Delete an existing document. If there is no document with the specified `id`, do nothing.            |

Scala
: @@snip [snip](/elasticsearch/src/test/scala/docs/scaladsl/ElasticsearchSpec.scala) { #multiple-operations }

Java
: @@snip [snip](/elasticsearch/src/test/java/docs/javadsl/ElasticsearchTest.java) { #multiple-operations }

### Source configuration

We can configure the source by `ElasticsearchSourceSettings`.

Scala
: @@snip [snip](/elasticsearch/src/test/scala/docs/scaladsl/ElasticsearchSpec.scala) { #source-settings }

Java
: @@snip [snip](/elasticsearch/src/test/java/docs/javadsl/ElasticsearchTest.java) { #source-settings }


| Parameter              | Default | Description                                                                                                              |
| ---------------------- | ------- | ------------------------------------------------------------------------------------------------------------------------ |
| bufferSize             | 10      | `ElasticsearchSource` retrieves messages from Elasticsearch by scroll scan. This buffer size is used as the scroll size. | 
| includeDocumentVersion | false   | Tell Elasticsearch to return the documents `_version` property with the search results. See [Version](http://nocf-www.elastic.co/guide/en/elasticsearch/reference/current/search-request-version.html) and [Optimistic Concurrenct Control](https://www.elastic.co/guide/en/elasticsearch/guide/current/optimistic-concurrency-control.html) to know about this property. |
| scrollDuration         | 5 min   | `ElasticsearchSource`  retrieves messages from Elasticsearch by scroll scan. This parameter is used as a scroll value. See [Time units](https://www.elastic.co/guide/en/elasticsearch/reference/current/common-options.html#time-units) for supported units.                |


### Sink and flow configuration

Sinks and flows are configured with `ElasticsearchWriteSettings`.

Scala
: @@snip [snip](/elasticsearch/src/test/scala/docs/scaladsl/ElasticsearchSpec.scala) { #sink-settings }

Java
: @@snip [snip](/elasticsearch/src/test/java/docs/javadsl/ElasticsearchTest.java) { #sink-settings }


| Parameter           | Default | Description                                                                                            |
| ------------------- | ------- | ------------------------------------------------------------------------------------------------------ |
| bufferSize          | 10      | Flow and Sink batch messages to bulk requests when back-pressure applies.                             |
| versionType         | None    | If set, `ElasticsearchSink` uses the chosen versionType to index documents. See [Version types](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-index_.html#_version_types) for accepted settings. |
| retryLogic | No retries | See below |


A bulk request might fail partially for some reason. To retry failed writes to Elasticsearch, a `RetryLogic` can be specified. 

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


## Elasticsearch as Flow

You can also build flow stages with @apidoc[ElasticsearchFlow$].
The API is similar to creating Sinks.

Scala
: @@snip [snip](/elasticsearch/src/test/scala/docs/scaladsl/ElasticsearchSpec.scala) { #run-flow }

Java
: @@snip [snip](/elasticsearch/src/test/java/docs/javadsl/ElasticsearchTest.java) { #run-flow }


### Storing documents from Strings

Elasticsearch requires the documents to be properly formatted JSON. If your data is available as JSON in Strings, you may use the pre-defined `StringMessageWriter` to avoid any conversions. For any other JSON technologies, implement a @scala[`MessageWriter[T]`]@java[`MessageWriter<T>`].

Scala
: @@snip [snip](/elasticsearch/src/test/scala/docs/scaladsl/ElasticsearchSpec.scala) { #string }

Java
: @@snip [snip](/elasticsearch/src/test/java/docs/javadsl/ElasticsearchTest.java) { #string }



### Passing data through ElasticsearchFlow

When streaming documents from Kafka, you might want to commit to Kafka **AFTER** the document has been written to Elastic.

Scala
: @@snip [snip](/elasticsearch/src/test/scala/docs/scaladsl/ElasticsearchSpec.scala) { #kafka-example }

Java
: @@snip [snip](/elasticsearch/src/test/java/docs/javadsl/ElasticsearchTest.java) { #kafka-example }


### Specifying custom index-name for every document

When working with index-patterns using wildcards, you might need to specify a custom
index-name for each document:

Scala
: @@snip [snip](/elasticsearch/src/test/scala/docs/scaladsl/ElasticsearchSpec.scala) { #custom-index-name-example }

Java
: @@snip [snip](/elasticsearch/src/test/java/docs/javadsl/ElasticsearchTest.java) { #custom-index-name-example }


### Specifying custom metadata for every document

In some cases you might want to specify custom metadata per document you are inserting, for example a `pipeline`, 
this can be done like so:

Scala
: @@snip [snip](/elasticsearch/src/test/scala/docs/scaladsl/ElasticsearchSpec.scala) { #custom-metadata-example }

Java
: @@snip [snip](/elasticsearch/src/test/java/docs/javadsl/ElasticsearchTest.java) { #custom-metadata-example }

 
### More custom searching

The easiest way of using ElasticSearch-source, is to just specify the query-param. Sometimes you need more control,
like specifying which fields to return and so on. In such cases you can instead use 'searchParams' instead:

Scala
: @@snip [snip](/elasticsearch/src/test/scala/docs/scaladsl/ElasticsearchSpec.scala) { #custom-search-params }

Java
: @@snip [snip](/elasticsearch/src/test/java/docs/javadsl/ElasticsearchTest.java) { #custom-search-params }
