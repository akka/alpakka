# Text and charsets

The text flows allow to translate a stream of text data according to the used 
character sets. It supports conversion between ByteString and String, as well 
as conversion of the character set in binary text data in the form of ByteStrings.

The main use case for these flows is the transcoding of text read from a
source with a certain character set, which may not be usable with other flows
or sinks. For example may CSV data arrive in UTF-16 encoding, but the Alpakka CSV
parser does only support UTF-8.

@@project-info{ projectId="text" }


## Artifacts

The Akka dependencies are available from Akka's library repository. To access them there, you need to configure the URL for this repository.

@@repository [Maven,sbt,Gradle] {
id="akka-repository"
name="Akka library repository"
url="https://repo.akka.io/maven"
}

Additionally, add the dependencies as below.

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-text_$scala.binary.version$
  version=$project.version$
  symbol2=AkkaVersion
  value2=$akka.version$
  group2=com.typesafe.akka
  artifact2=akka-stream_$scala.binary.version$
  version2=AkkaVersion
}


The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="text" }


## Text transcoding

The text transcoding flow converts incoming binary text data (ByteString) to binary text
data of another character encoding. 

The flow fails with an @javadoc[UnmappableCharacterException](java.nio.charset.UnmappableCharacterException), 
if a character is not representable in the targeted character set.

Scala
: @@snip [snip](/text/src/test/scala/docs/scaladsl/CharsetCodingFlowsDoc.scala) { #transcoding }

Java
: @@snip [snip](/text/src/test/java/docs/javadsl/CharsetCodingFlowsDoc.java) { #transcoding }

## Text encoding

The text encoding flow converts incoming Strings to binary text data (ByteString) with the 
given character encoding. 

The flow fails with an @javadoc[UnmappableCharacterException](java.nio.charset.UnmappableCharacterException), 
if a character is not representable in the targeted character set.

Scala
: @@snip [snip](/text/src/test/scala/docs/scaladsl/CharsetCodingFlowsDoc.scala) { #encoding }

Java
: @@snip [snip](/text/src/test/java/docs/javadsl/CharsetCodingFlowsDoc.java) { #encoding }

## Text decoding

The text decoding flow converts incoming ByteStrings to Strings using the given 
character encoding. 

Scala
: @@snip [snip](/text/src/test/scala/docs/scaladsl/CharsetCodingFlowsDoc.scala) { #decoding }

Java
: @@snip [snip](/text/src/test/java/docs/javadsl/CharsetCodingFlowsDoc.java) { #decoding }
