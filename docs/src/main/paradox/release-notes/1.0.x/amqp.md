# AMQP

## 1.0.1

No changes.

[*closed in 1.0.1*](https://github.com/akka/alpakka/issues?q=is%3Aclosed+milestone%3A1.0.1+label%3Ap%3Aamqp)


## 1.0.0 (changes since 0.20)

* Add support for all SSL configuration variants [#1146](https://github.com/akka/alpakka/pull/1146) **api-change**
* Uncase class connection providers [#1163](https://github.com/akka/alpakka/pull/1163)   
* Refactor case classes to private classes  [#1125](https://github.com/akka/alpakka/pull/1125)  **api-change**  
* Allow setting of `autoAck` on underlying `channel.basicConsume` call [#1001](https://github.com/akka/alpakka/pull/1001)   
* Disable automatic recovery by default [#1273](https://github.com/akka/alpakka/pull/1273)   
* Fix `withSSLContext` [#1430](https://github.com/akka/alpakka/pull/1430)   
* Renaming of stream element classes [#1524](https://github.com/akka/alpakka/pull/1524)  **api-change**  

| new name | earlier name |
|----------|--------------|
| `ReadResult` | `IncomingMessage` |
| `CommittableReadResult` | `CommittableIncomingMessage` |
| `WriteMessage` | `OutgoingMessage` |
| `AmqpWriteSettings` | `AmqpSinkSettings` |


[*closed in 1.0.0*](https://github.com/akka/alpakka/issues?q=is%3Aclosed+milestone%3A1.0.0+label%3Ap%3Aamqp)
[*closed in 1.0-RC1*](https://github.com/akka/alpakka/issues?q=is%3Aclosed+milestone%3A1.0-RC1+label%3Ap%3Aamqp)
[*closed in 1.0-M3*](https://github.com/akka/alpakka/issues?q=is%3Aclosed+milestone%3A1.0-M3+label%3Ap%3Aamqp)
[*closed in 1.0-M2*](https://github.com/akka/alpakka/issues?q=is%3Aclosed+milestone%3A1.0-M2+label%3Ap%3Aamqp)
[*closed in 1.0-M1*](https://github.com/akka/alpakka/issues?q=is%3Aclosed+milestone%3A1.0-M1+label%3Ap%3Aamqp)
