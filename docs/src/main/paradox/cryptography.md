# Dependency Free Cryptography
Providing support for encrytion and decryption without any dependencies by making use of underlying Java cryptographic libraries.
Provide are four different flows to encrypt and decrypt @scaladoc[ByteString](akka.util.ByteString) to @scaladoc[ByteString](akka.util.ByteString). Using symmetric and asymmetric algorithms.

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-cryptography_$scalaBinaryVersion$
  version=$version$
}

## Usage

Scala
: @@snip ($alpakka$/cryptography/src/test/scala/akka/stream/alpakka/cryptography/ExampleSpec.scala) { #init-client }

: @@snip ($alpakka$/cryptography/src/test/scala/akka/stream/alpakka/cryptography/ExampleSpec.scala) { #scala-symmetric }

: @@snip ($alpakka$/cryptography/src/test/scala/akka/stream/alpakka/cryptography/ExampleSpec.scala) { #scala-asymmetric }
