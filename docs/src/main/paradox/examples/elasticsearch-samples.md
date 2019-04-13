# Elasticsearch

### Example: Index all data from an RDBMS table into Elasticsearch

- Instantiate a Slick database session using the config parameters defined in key `slick-h2-mem` 
and mount closing it on shutdown of the Actor System (1)
- Scala only: Slick definition of the MOVIE table (2)
- Class that holds the Movie data (3)
- Instantiate Elastic REST client (4)
- Scala: Instantiate the Spray json format that converts the `Movie` case class to json (5)
- Java: Instantiate the Jackson Object mapper that converts the `Movie` class to json (5)
- Construct the Slick `Source` for the H2 table and query all data in the table (6)
- Scala only: Map each tuple into a `Movie` case class instance (7)
- The first argument of the `IncomingMessage` is the *id* of the document. Replace with `None` if you would Elastic to generate one (8)
- Prepare the Elastic `Sink` that the data needs to be drained to (9)
- Close the Elastic client upon completion of indexing the data (10)

Scala
: @@snip [snip](/doc-examples/src/main/scala/elastic/FetchUsingSlickAndStreamIntoElastic.scala) { #sample }

Java
: @@snip [snip](/doc-examples/src/main/java/elastic/FetchUsingSlickAndStreamIntoElasticInJava.java) { #sample }


### Example: Read from a Kafka topic and publish to Elasticsearch

- Configure Kafka consumer (1)
- Data class mapped to Elasticsearch (2)
- Spray JSON conversion for the data class (3)
- Elasticsearch client setup (4)
- Kafka consumer with committing support (5)
- Use `FlowWithContext` to focus on `ConsumerRecord` (6)
- Parse message from Kafka to `Movie` and create Elasticsearch write message (7)
- Use `createWithContext` to use an Elasticsearch flow with context-support (so it passes through the Kafka committ offset) (8)
- React on write errors (9)
- Make the context visible again and just keep the committable offset (10)
- Let the `Committer.sink` aggregate commits to batches and commit to Kafka (11)
- Combine consumer control and stream completion into `DrainingControl` (12)



Scala
: @@snip [snip](/doc-examples/src/main/scala/elastic/KafkaToElastic.scala) { #imports #kafka-setup #es-setup #flow }



### Running the example code

This example is contained in a stand-alone runnable main, it can be run
 from `sbt` like this:
 

Scala
:   ```
    sbt
    > doc-examples/run
    ```
