# Elasticsearch Connector

The Elasticsearch connector provides Akka Stream sources and sinks for Elasticsearch.

For more information about Elasticsearch please visit the [Elasticsearch documentation](https://www.elastic.co/guide/index.html).


### Reported issues

[Tagged issues at Github](https://github.com/akka/alpakka/labels/p%3Aelasticsearch)


## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-elasticsearch_$scalaBinaryVersion$
  version=$version$
}

## Set up REST client

Sources, Flows and Sinks provided by this connector need a prepared `org.elasticsearch.client.RestClient` to
access to Elasticsearch.

Scala
: @@snip ($alpakka$/elasticsearch/src/test/scala/akka/stream/alpakka/elasticsearch/ElasticsearchSpec.scala) { #init-client }

Java
: @@snip ($alpakka$/elasticsearch/src/test/java/akka/stream/alpakka/elasticsearch/ElasticsearchTest.java) { #init-client }


## Elasticsearch as Source and Sink

Now we can stream messages from or to Elasticsearch by providing the `RestClient` to the
@scala[@scaladoc[ElasticsearchSource](akka.stream.alpakka.elasticsearch.scaladsl.ElasticsearchSource$)]
@java[@scaladoc[ElasticsearchSource](akka.stream.alpakka.elasticsearch.javadsl.ElasticsearchSource$)]
or the
@scala[@scaladoc[ElasticsearchSink](akka.stream.alpakka.elasticsearch.scaladsl.ElasticsearchSink$).]
@java[@scaladoc[ElasticsearchSink](akka.stream.alpakka.elasticsearch.javadsl.ElasticsearchSink$).]


Scala
: @@snip ($alpakka$/elasticsearch/src/test/scala/akka/stream/alpakka/elasticsearch/ElasticsearchSpec.scala) { #define-class }

Java
: @@snip ($alpakka$/elasticsearch/src/test/java/akka/stream/alpakka/elasticsearch/ElasticsearchTest.java) { #define-class }

### With typed source

Use `ElasticsearchSource.typed` and `ElasticsearchSink.create` to create source and sink.
@scala[The data is converted to and from JSON by Spray JSON.]
@java[The data is converted to and from JSON by Jackson's ObjectMapper.]

Scala
: @@snip ($alpakka$/elasticsearch/src/test/scala/akka/stream/alpakka/elasticsearch/ElasticsearchSpec.scala) { #run-typed }

Java
: @@snip ($alpakka$/elasticsearch/src/test/java/akka/stream/alpakka/elasticsearch/ElasticsearchTest.java) { #run-typed }

### With JSON source

Use `ElasticsearchSource.create` and `ElasticsearchSink.create` to create source and sink.

Scala
: @@snip ($alpakka$/elasticsearch/src/test/scala/akka/stream/alpakka/elasticsearch/ElasticsearchSpec.scala) { #run-jsobject }

Java
: @@snip ($alpakka$/elasticsearch/src/test/java/akka/stream/alpakka/elasticsearch/ElasticsearchTest.java) { #run-jsobject }


### Configuration

We can configure the source by `ElasticsearchSourceSettings`.

Scala
: @@snip ($alpakka$/elasticsearch/src/test/scala/akka/stream/alpakka/elasticsearch/ElasticsearchSpec.scala) { #source-settings }

Java
: @@snip ($alpakka$/elasticsearch/src/test/java/akka/stream/alpakka/elasticsearch/ElasticsearchTest.java) { #source-settings }


| Parameter  | Default | Description                                                                                                              |
| ---------- | ------- | ------------------------------------------------------------------------------------------------------------------------ |
| bufferSize | 10      | `ElasticsearchSource` retrieves messages from Elasticsearch by scroll scan. This buffer size is used as the scroll size. | 


Also, we can configure the sink by `ElasticsearchSinkSettings`.

Scala
: @@snip ($alpakka$/elasticsearch/src/test/scala/akka/stream/alpakka/elasticsearch/ElasticsearchSpec.scala) { #sink-settings }

Java
: @@snip ($alpakka$/elasticsearch/src/test/java/akka/stream/alpakka/elasticsearch/ElasticsearchTest.java) { #sink-settings }


| Parameter           | Default | Description                                                                                            |
| ------------------- | ------- | ------------------------------------------------------------------------------------------------------ |
| bufferSize          | 10      | `ElasticsearchSink` puts messages by one bulk request per messages of this buffer size.                |
| retryInterval       | 5000    | When a request is failed, `ElasticsearchSink` retries that request after this interval (milliseconds). |
| maxRetry            | 100     | `ElasticsearchSink` give up and fails the stage if it gets this number of consective failures.         | 
| retryPartialFailure | true    | A bulk request might fails partially for some reason. If this parameter is true, then `ElasticsearchSink` retries to request these failed messages. Otherwise, failed messages are discarded (or pushed to downstream if you use `ElasticsearchFlow` instead of the sink). |
| docAsUpsert         | false   | If this parameter is true, `ElasticsearchSink` uses the upsert method to index documents. By default, documents are added using the standard index method (which create or replace). |
| versionType         | None    | If set, `ElasticsearchSink` uses the chosen versionType to index documents. See [Version types](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-index_.html#_version_types) for accepted settings. |

## Elasticsearch as Flow

You can also build flow stages with
@scala[@scaladoc[ElasticsearchFlow](akka.stream.alpakka.elasticsearch.scaladsl.ElasticsearchFlow$).]
@java[@scaladoc[ElasticsearchFlow](akka.stream.alpakka.elasticsearch.javadsl.ElasticsearchFlow$).]
The API is similar to creating Sinks.

Scala
: @@snip ($alpakka$/elasticsearch/src/test/scala/akka/stream/alpakka/elasticsearch/ElasticsearchSpec.scala) { #run-flow }

Java
: @@snip ($alpakka$/elasticsearch/src/test/java/akka/stream/alpakka/elasticsearch/ElasticsearchTest.java) { #run-flow }

### Passing data through ElasticsearchFlow

When streaming documents from Kafka, you might want to commit to Kafka **AFTER** the document has been written to Elastic.

Scala
: @@snip ($alpakka$/elasticsearch/src/test/scala/akka/stream/alpakka/elasticsearch/ElasticsearchSpec.scala) { #kafka-example }

Java
: @@snip ($alpakka$/elasticsearch/src/test/java/akka/stream/alpakka/elasticsearch/ElasticsearchTest.java) { #kafka-example }

### Specifying custom index-name for every document

When working with index-patterns using wildcards, you might need to specify a custom
index-name for each document:

Scala
: @@snip ($alpakka$/elasticsearch/src/test/scala/akka/stream/alpakka/elasticsearch/ElasticsearchSpec.scala) { #custom-index-name-example }
 
### More custom searching

The easiest way of using ElasticSearch-source, is to just specify the query-param. Sometimes you need more control,
like specifying which fields to return and so on. In such cases you can instead use 'searchParams' instead:

Scala
: @@snip ($alpakka$/elasticsearch/src/test/scala/akka/stream/alpakka/elasticsearch/ElasticsearchSpec.scala) { #custom-search-params }

Java
: @@snip ($alpakka$/elasticsearch/src/test/java/akka/stream/alpakka/elasticsearch/ElasticsearchTest.java) { #custom-search-params }


### Running the example code

The code in this guide is part of runnable tests of this project. You are welcome to edit the code and run it in sbt.

Scala
:   ```
    sbt
    > elasticsearch/testOnly *.ElasticsearchSpec
    ```

Java
:   ```
    sbt
    > elasticsearch/testOnly *.ElasticsearchTest
    ```
