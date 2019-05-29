# Elasticsearch

## 1.0.2

No changes.

[*closed in 1.0.2*](https://github.com/akka/alpakka/issues?q=is%3Aclosed+milestone%3A1.0.2+label%3Ap%3Aelasticsearch)


## 1.0.1

No changes.

[*closed in 1.0.1*](https://github.com/akka/alpakka/issues?q=is%3Aclosed+milestone%3A1.0.1+label%3Ap%3Aelasticsearch)


## 1.0.0 (changes since 0.20)

* Custom per message elastic search metadata [#1161](https://github.com/akka/alpakka/pull/1161)   

* Use `_doc` as type in Elasticsearch tests [#1124](https://github.com/akka/alpakka/pull/1124)   

* Update Elasticsearch docs to describe incoming message types [#1118](https://github.com/akka/alpakka/pull/1118)   

* Support update and delete by incoming message types [#1104](https://github.com/akka/alpakka/pull/1104)   

* Add missing type declaration to keep source uniformity [#1101](https://github.com/akka/alpakka/pull/1101)   

* `failStage` should happen when `shouldRetry` returns false [#1302](https://github.com/akka/alpakka/pull/1302)   

* towards 1.0 [#1179](https://github.com/akka/alpakka/pull/1179)  **api-change**  
    * Major API changes

    | <= v0.20 | now |
    |----------|-----|
    | `IncomingMessage` | `WriteMessage` (non case class) |
    | `IncomingIndexMessage` | use `WriteMessage.createIndexMessage` |
    | `IncomingUpdateMessage` | use `WriteMessage.createUpdateMessage` |
    | `IncomingUpsertMessage` | use `WriteMessage.createUpsertMessage` |
    | `IncomingDeleteMessage` | use `WriteMessage.createDeleteMessage` |
    | `IncomingMessageResult` | `WriteResult` |
    | `OutgoingMessage` | `ReadResult` |
    
    * `ElasticsearchSinkSettings` is now called `ElasticsearchWriteSettings` and no case class anymore. The fields to specify retrying are replaced by `RetryLogic` with the two implementations `RetryNever` (default) and `RetryAtFixedRate`.

    * `ElasticsearchSourceSettings` is no case class anymore.

    * To support implementing stream elements created by the Elasticsearch stages, `akka.stream.alpakka.elasticsearch.testkit.MessageFactory` offers factory methods.

* Making the scroll value configurable [#1366](https://github.com/akka/alpakka/pull/1366)   

* introduce `withContext` flow [#1523](https://github.com/akka/alpakka/pull/1523)   

* Add CreteCreateMessage which supports Elasticsearch op_type 'create' [#1510](https://github.com/akka/alpakka/pull/1510)   

* Make flows type signature simpler to prepare for `withContext` [#1515](https://github.com/akka/alpakka/pull/1515)  **api-change**  

* null check for index and type name on source and flow [#1478](https://github.com/akka/alpakka/pull/1478)   

[*closed in 1.0.0*](https://github.com/akka/alpakka/issues?q=is%3Aclosed+milestone%3A1.0.0+label%3Ap%3Aelasticsearch)
[*closed in 1.0-RC1*](https://github.com/akka/alpakka/issues?q=is%3Aclosed+milestone%3A1.0-RC1+label%3Ap%3Aelasticsearch)
[*closed in 1.0-M3*](https://github.com/akka/alpakka/issues?q=is%3Aclosed+milestone%3A1.0-M3+label%3Ap%3Aelasticsearch)
[*closed in 1.0-M2*](https://github.com/akka/alpakka/issues?q=is%3Aclosed+milestone%3A1.0-M2+label%3Ap%3Aelasticsearch)
[*closed in 1.0-M1*](https://github.com/akka/alpakka/issues?q=is%3Aclosed+milestone%3A1.0-M1+label%3Ap%3Aelasticsearch)
