# Reference

This is the reference documentation for an Alpakka connector. This section should contain
a general overview of the connector and mention the libraries and APIs that the connector
is using. Also it should link to external resources that might help to learn
about the technology the connector is using.

### Reported issues

[Tagged issues at Github](https://github.com/akka/alpakka/labels/p%3Areference)

## Artifacts

The Akka dependencies are available from Akka's library repository. To access them there, you need to configure the URL for this repository.

@@repository [sbt,Maven,Gradle] {
id="akka-repository"
name="Akka library repository"
url="https://repo.akka.io/maven"
}

Additionally, add the dependencies as below.

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-reference_$scala.binary.version$
  version=$project.version$
  symbol2=AkkaVersion
  value2=$akka.version$
  group2=com.typesafe.akka
  artifact2=akka-stream_$scala.binary.version$
  version2=AkkaVersion
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="reference" }


## Reading messages

Give a brief description of the usage of this connector. If you want to mention a
class name, make sure to link to the API docs:
@scaladoc[ReferenceReadResult](akka.stream.alpakka.reference.ReferenceReadResult).

If any of the API classes are different between Scala and Java, link to both API docs:
@scala[@scaladoc[Reference](akka.stream.alpakka.reference.scaladsl.Reference$)]
@java[@scaladoc[Reference](akka.stream.alpakka.reference.javadsl.Reference$)].

Show an example code snippet of how a source of this connector can be created.

Scala
: @@snip [snip](/reference/src/test/scala/docs/scaladsl/ReferenceSpec.scala) { #source }

Java
: @@snip [snip](/reference/src/test/java/docs/javadsl/ReferenceTest.java) { #source }

Wrap language specific text with language specific directives,
like @scala[`@scala` for Scala specific text]@java[`@java` for Java specific text].

## Writing messages

Show an example code snippet of how a flow of this connector can be created.

Scala
: @@snip [snip](/reference/src/test/scala/docs/scaladsl/ReferenceSpec.scala) { #flow }

Java
: @@snip [snip](/reference/src/test/java/docs/javadsl/ReferenceTest.java) { #flow }
