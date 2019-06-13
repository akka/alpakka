# JMS

## 1.0.2

No changes.

[*closed in 1.0.2*](https://github.com/akka/alpakka/issues?q=is%3Aclosed+milestone%3A1.0.2+label%3Ap%3Ajms)


## 1.0.1

No changes.

[*closed in 1.0.1*](https://github.com/akka/alpakka/issues?q=is%3Aclosed+milestone%3A1.0.1+label%3Ap%3Ajms)


## 1.0.0 (changes since 0.20)

* Support for durable subscriptions #966 [#1178](https://github.com/akka/alpakka/pull/1178)   

* Make sure the stream is there for all message acks (#974) [#1140](https://github.com/akka/alpakka/pull/1140)   

* Fixed visualization glitch in IBM MQ JmsConsumer doc section [#1103](https://github.com/akka/alpakka/pull/1103)   

* towards 1.0 [#1314](https://github.com/akka/alpakka/pull/1314)  **api-change**  
    * Preparation for Alpakka 1.0
    * Major overhaul of the documentation
    * Support settings via Config (defaults now in reference.conf)
    * Introduce `ByteStringMessage` (fixes #701)

* JMS producer multi session [#1148](https://github.com/akka/alpakka/pull/1148)   
    - JmsProducerStage now supports multiple sessions by behaving
      similarly to MapAsync
    - Producers are pooled and parallelism is controlled by how producers
      are returned to the pool.
    - Created a separate JmsMessageProducer class to keep the stage logic
      more focussed on coordinating elements within the stage.
    - Grouped the logic for creating the execution context to use in the
      producer stage as well as in the consumer stage.

* Jms producer retries [#1227](https://github.com/akka/alpakka/pull/1227)   

* Jms passthrough envelope [#1212](https://github.com/akka/alpakka/pull/1212)   

* Jms per message destination [#1181](https://github.com/akka/alpakka/pull/1181)   

* JMS: Introducing commit timeouts and use Future instead of BlockingQueue [#1189](https://github.com/akka/alpakka/pull/1189)   

* JMS: Adding connection timeouts and retries (fixes #1192 and #1193) [#1194](https://github.com/akka/alpakka/pull/1194)   

* Jms connection status [#1252](https://github.com/akka/alpakka/pull/1252)  **api-change**  

* JMS: Remove separate Envelope wrapper by doubling message classes [#1331](https://github.com/akka/alpakka/pull/1331)  **api-change**  

* use [jms-testkit] in JMS unit tests [#1401](https://github.com/akka/alpakka/pull/1401)   

* Fix JmsConnectionStatusSpec [#1337](https://github.com/akka/alpakka/pull/1337)   

* JMS: use full name of stage in logs [#1339](https://github.com/akka/alpakka/pull/1339)   

* jms-testkit 0.2.1 [#1495](https://github.com/akka/alpakka/pull/1495)   

* JMS: Adapt to JMS 2.0 API change in tests [#1496](https://github.com/akka/alpakka/pull/1496)   

* JMS: IBMMQ update docs [#1482](https://github.com/akka/alpakka/pull/1482)   

* JmsConnector: add JMS Destination name to log messages [#1483](https://github.com/akka/alpakka/pull/1483)   

[*closed in 1.0.0*](https://github.com/akka/alpakka/issues?q=is%3Aclosed+milestone%3A1.0.0+label%3Ap%3Ajms)
[*closed in 1.0-RC1*](https://github.com/akka/alpakka/issues?q=is%3Aclosed+milestone%3A1.0-RC1+label%3Ap%3Ajms)
[*closed in 1.0-M3*](https://github.com/akka/alpakka/issues?q=is%3Aclosed+milestone%3A1.0-M3+label%3Ap%3Ajms)
[*closed in 1.0-M2*](https://github.com/akka/alpakka/issues?q=is%3Aclosed+milestone%3A1.0-M2+label%3Ap%3Ajms)
[*closed in 1.0-M1*](https://github.com/akka/alpakka/issues?q=is%3Aclosed+milestone%3A1.0-M1+label%3Ap%3Ajms)
