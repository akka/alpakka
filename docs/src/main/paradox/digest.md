# Digest Connector

The Digest connector provides Akka Stream flow and sink to calculate a digest for a stream of ByteString.

## Artifacts

sbt
:   @@@vars
    ```scala
    libraryDependencies += "com.typesafe.akka" %% "akka-stream-alpakka-digest" % "$version$"
    ```
    @@@

Maven
:   @@@vars
    ```xml
    <dependency>
      <groupId>com.typesafe.akka</groupId>
      <artifactId>akka-stream-alpakka-digest_$scala.binaryVersion$</artifactId>
      <version>$version$</version>
    </dependency>
    ```
    @@@

Gradle
:   @@@vars
    ```gradle
    dependencies {
      compile group: "com.typesafe.akka", name: "akka-stream-alpakka-digest_$scala.binaryVersion$", version: "$version$"
    }
    ```
    @@@

## Usage
Given a stream of ByteString, the `akka.stream.alpakka.digest.DigestCalculator` calculates a digest given a certain Algorithm and
returns a `DigestResult`.

The contents of the DigestResult is:

@@snip[person.xsd](/../../../../digest/src/main/scala/akka/stream/alpakka/digest/DigestResult.scala)

The following algorithms are available:

@@snip[person.xsd](/../../../../digest/src/main/scala/akka/stream/alpakka/digest/Algorithm.scala)

The DigestCalculatorTest shows multiple ways to use the DigestCalculator and the result it generates:

@@snip[person.xsd](/../../../../digest/src/test/scala/akka/stream/alpakka/digest/DigestCalculatorTest.scala)


## Test

The code in this guide is part of runnable tests of this project. You are welcome to edit the code and run it in sbt.

Scala
:   ```
    sbt
    > digest/test
    ```

Java
:   ```
    sbt
    > digest/test
    ```
