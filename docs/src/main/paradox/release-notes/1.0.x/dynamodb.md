# Dynamo DB

## 1.0.1

No changes.

[*closed in 1.0.1*](https://github.com/akka/alpakka/issues?q=is%3Aclosed+milestone%3A1.0.1+label%3Ap%3Adynamodb)


## 1.0.0 (changes since 0.20)

* Refactor DynamoDb to factories taking client [#1220](https://github.com/akka/alpakka/pull/1220)   

* towards 1.0 [#1206](https://github.com/akka/alpakka/pull/1206)  **api-change** 
    * A few classes used in the public API moved out of the `impl` package (eg. `DynamoSettings`)
    * `DynamoSettings` now supports configuration via `withXyz` methods (in addition to reading from `Config`)
    * To use unencrypted http, `tls` needs explicitly to be set to false. 

* Base URL should be overridable in `DynamoClientImpl` [#1384](https://github.com/akka/alpakka/pull/1384)   

* Add maxOpenRequests param in DynamoConfig [#1545](https://github.com/akka/alpakka/pull/1545)  **enhancement**  

[*closed in 1.0.0*](https://github.com/akka/alpakka/issues?q=is%3Aclosed+milestone%3A1.0.0+label%3Ap%3Adynamodb)
[*closed in 1.0-RC1*](https://github.com/akka/alpakka/issues?q=is%3Aclosed+milestone%3A1.0-RC1+label%3Ap%3Adynamodb)
[*closed in 1.0-M3*](https://github.com/akka/alpakka/issues?q=is%3Aclosed+milestone%3A1.0-M3+label%3Ap%3Adynamodb)
[*closed in 1.0-M2*](https://github.com/akka/alpakka/issues?q=is%3Aclosed+milestone%3A1.0-M2+label%3Ap%3Adynamodb)
[*closed in 1.0-M1*](https://github.com/akka/alpakka/issues?q=is%3Aclosed+milestone%3A1.0-M1+label%3Ap%3Adynamodb)
