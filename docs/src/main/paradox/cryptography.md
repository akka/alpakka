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
The examples need an @scaladoc[ActorSystem](akka.actor.ActorSystem) and an @scaladoc[ActorMaterializer](akka.stream.ActorMaterializer).
Scala
: @@snip ($alpakka$/cryptography/src/test/scala/akka/stream/alpakka/cryptography/scaladsl/ExampleSpec.scala) { #init-client }

Java
: @@snip ($alpakka$/cryptography/src/test/java/akka/stream/alpakka/cryptography/javadsl/ExampleTest.java) { #init-client }

An example of symmetric encryption and decryption using an AES key.
Scala
: @@snip ($alpakka$/cryptography/src/test/scala/akka/stream/alpakka/cryptography/scaladsl/ExampleSpec.scala) { #scala-symmetric }

Java
: @@snip ($alpakka$/cryptography/src/test/java/akka/stream/alpakka/cryptography/javadsl/ExampleTest.java) { #java-symmetric }

An example of asymmetric encryption and decryption using an RSA key pair.
Scala
: @@snip ($alpakka$/cryptography/src/test/scala/akka/stream/alpakka/cryptography/scaladsl/ExampleSpec.scala) { #scala-asymmetric }

Java
: @@snip ($alpakka$/cryptography/src/test/java/akka/stream/alpakka/cryptography/javadsl/ExampleTest.java) { #java-asymmetric }

