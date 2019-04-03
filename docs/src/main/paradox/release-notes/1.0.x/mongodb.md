# MongoDB

## 1.0.0 (changes since 0.20)

* Update driver to 2.4.2 [#1234](https://github.com/akka/alpakka/pull/1234)

* Towards Alpakka 1.0 [#1514](https://github.com/akka/alpakka/pull/1514)
    * Adds javadsl for API parity.
    * To reuse the implementation, MongoDB connector was switched from scala MongoDB driver to the Reactive Streams one.
    * BSON construction API can still be used from the scala driver, but that jar will need to be pulled in separately, as it is done in tests.

[*closed in 1.0.0*](https://github.com/akka/alpakka/issues?q=is%3Aclosed+milestone%3A1.0.0+label%3Ap%3Amongodb)
[*closed in 1.0-RC1*](https://github.com/akka/alpakka/issues?q=is%3Aclosed+milestone%3A1.0-RC1+label%3Ap%3Amongodb)
[*closed in 1.0-M3*](https://github.com/akka/alpakka/issues?q=is%3Aclosed+milestone%3A1.0-M3+label%3Ap%3Amongodb)
[*closed in 1.0-M2*](https://github.com/akka/alpakka/issues?q=is%3Aclosed+milestone%3A1.0-M2+label%3Ap%3Amongodb)
[*closed in 1.0-M1*](https://github.com/akka/alpakka/issues?q=is%3Aclosed+milestone%3A1.0-M1+label%3Ap%3Amongodb)
