# Text and charsets

The text flows allow to translate a stream of text data according to the used 
character sets. It supports conversion between ByteString and String, as well 
as conversion of the character set in binary text data in the form of ByteStrings.

The main use case for these flows is the transcoding of text read from a
source with a certain character set, which may not be usable with other flows
or sinks. For example may CSV data arrive in UTF-16 encoding, but the Alpakka CSV
parser does only support UTF-8.

### Reported issues

[Tagged issues at Github](https://github.com/akka/alpakka/labels/p%3Atext)


## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-text_$scalaBinaryVersion$
  version=$version$
}


## Text transcoding

The text transcoding flow converts incoming binary text data (ByteString) to binary text
data of another character encoding. 

The flow fails with an @javadoc[UnmappableCharacterException](java.nio.charset.UnmappableCharacterException), 
if a character is not representable in the targeted character set.

Scala
: @@snip ($alpakka$/text/src/test/scala/akka/stream/alpakka/text/scaladsl/CharsetCodingFlowsSpec.scala) { #transcoding }

Java
: @@snip ($alpakka$/text/src/test/java/akka/stream/alpakka/text/javadsl/CharsetCodingFlowsTest.java) { #transcoding }

@scala[@github[Source on Github](/text/src/test/scala/akka/stream/alpakka/text/scaladsl/CharsetCodingFlowsSpec.scala)]
@java[@github[Source on Github](/text/src/test/java/akka/stream/alpakka/text/javadsl/CharsetCodingFlowsTest.java)]


## Text encoding

The text encoding flow converts incoming Strings to binary text data (ByteString) with the 
given character encoding. 

The flow fails with an @javadoc[UnmappableCharacterException](java.nio.charset.UnmappableCharacterException), 
if a character is not representable in the targeted character set.

Scala
: @@snip ($alpakka$/text/src/test/scala/akka/stream/alpakka/text/scaladsl/CharsetCodingFlowsSpec.scala) { #encoding }

Java
: @@snip ($alpakka$/text/src/test/java/akka/stream/alpakka/text/javadsl/CharsetCodingFlowsTest.java) { #encoding }

@scala[@github[Source on Github](/text/src/test/scala/akka/stream/alpakka/text/scaladsl/CharsetCodingFlowsSpec.scala)]
@java[@github[Source on Github](/text/src/test/java/akka/stream/alpakka/text/javadsl/CharsetCodingFlowsTest.java)]


## Text decoding

The text decoding flow converts incoming ByteStrings to Strings using the given 
character encoding. As some characters may not exist in 't be encoded with 

Scala
: @@snip ($alpakka$/text/src/test/scala/akka/stream/alpakka/text/scaladsl/CharsetCodingFlowsSpec.scala) { #decoding }

Java
: @@snip ($alpakka$/text/src/test/java/akka/stream/alpakka/text/javadsl/CharsetCodingFlowsTest.java) { #decoding }

@scala[@github[Source on Github](/text/src/test/scala/akka/stream/alpakka/text/scaladsl/CharsetCodingFlowsSpec.scala)]
@java[@github[Source on Github](/text/src/test/java/akka/stream/alpakka/text/javadsl/CharsetCodingFlowsTest.java)]


