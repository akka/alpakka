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

This example is now available in the [Alpakka Samples](https://akka.io/alpakka-samples/kafka-to-elasticsearch/) project.

### Running the example code

This example is contained in a stand-alone runnable main, it can be run
 from `sbt` like this:
 

Scala
:   ```
    sbt
    > doc-examples/run
    ```
