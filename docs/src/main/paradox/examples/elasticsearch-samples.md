# Elasticsearch examples

### Example: Index all data from an RDBMS table into Elasticsearch

- Instantiate a Slick database session using the config parameters definited in key `slick-h2-mem` (1)
- Slick definition of the MOVIE table (2)
- case class that holds the results of the Slick query. (3)
- Construct the Slick `Source` for the H2 table (4)
- Query all data in the table (5)
- Map each tuple into a `Movie` case class instance (6)
- Instantiate Elastic REST client (7)
- Instantiate the Spray json format that converts the `Movie` case class to json (8)
- Prepare the Elastic `Sink` that the data needs to be drained to (9)
- The first argument of the `IncomingMessage` is the *id* of the document. Replace with `None` if you would Elastic to generate one (10)
- Close the Slick session and the Elastic client after upon completion of indexing the data (11)

Scala
: @@snip ($alpakka$/doc-examples/src/main/scala/elastic/FetchUsingSlickAndStreamIntoElastic.scala) { #sample }

@github[Full source](/doc-examples/src/main/scala/elastic/FetchUsingSlickAndStreamIntoElastic.scala) { #sample }


### Running the example code

This example is contained in a stand-alone runnable main, it can be run
 from `sbt` like this:
 

Scala
:   ```
    sbt
    > doc-examples/run
    ```
