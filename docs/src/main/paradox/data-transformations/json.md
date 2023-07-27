# JSON

## JSON Framing

Use Akka Stream JsonFraming to split a stream of @apidoc[akka.util.ByteString] elements into 
ByteString snippets of valid JSON objects. 

See @apidoc[JsonFraming$]


@extref:[Akka documentation](akka:stream/stream-io.html#using-framing-in-your-protocol)



```
JsonFraming.objectScanner(maximumObjectLength: Int): Flow[ByteString, ByteString, NotUsed]
```

Returns a Flow that implements a "brace counting" based framing stage for emitting valid JSON chunks.

Typical examples of data that one may want to frame using this stage include:

**Very large arrays**:

```
[{"id": 1}, {"id": 2}, [...], {"id": 999}]
```

**Multiple concatenated JSON objects** (with, or without commas between them):

```
{"id": 1}, {"id": 2}, [...], {"id": 999}
```

The framing works independently of formatting, i.e. it will still emit valid JSON elements even if two 
elements are separated by multiple newlines or other whitespace characters. And of course is insensitive 
(and does not impact the emitting frame) to the JSON object's internal formatting.

## Streaming of nested structures

The method above is great for a stream of "flat" JSON objects (an array or just a stream of objects) but
doesn't work for the many use-cases that involve a nested structure. A common example is the response of a 
database, which might look more like this:

```
{
  "size": 100,
  "rows": [
    {"id": 1, "doc": {}}
    {"id": 2, "doc": {}}
    ...
  ]
}
```

The JSON reading module offers a flow, which allows to stream specific parts of that JSON structure.
In this particular example, only the `rows` array is interesting for the application, more specifically
even: only the `doc` inside each element of the array.


@@project-info{ projectId="json-streaming" }


### Artifacts

The Akka dependencies are available from Akka's library repository. To access them there, you need to configure the URL for this repository.

@@repository [Maven,sbt,Gradle] {
id="akka-repository"
name="Akka library repository"
url="https://repo.akka.io/maven"
}

Additionally, add the dependencies as below.

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-json-streaming_$scala.binary.version$
  version=$project.version$
  symbol2=AkkaVersion
  value2=$akka.version$
  group2=com.typesafe.akka
  artifact2=akka-stream_$scala.binary.version$
  version2=AkkaVersion
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="json-streaming" }


### Example

To define which parts of the structure you want to stream the module supports
[JsonPath notation](https://github.com/jsurfer/JsonSurfer#what-is-jsonpath). For example:

- Stream all elements of the nested `rows` array: `$.rows[*]`
- Stream the value of `doc` of each element in the array: `$.rows[*].doc`

To extract the information needed, run a stream through the `JsonReader.select` flow.

Scala
: @@snip [snip](/json-streaming/src/test/scala/docs/scaladsl/JsonReaderTest.scala) { #usage }

Java
: @@snip [snip](/json-streaming/src/test/java/docs/javadsl/JsonReaderUsageTest.java) { #usage }
