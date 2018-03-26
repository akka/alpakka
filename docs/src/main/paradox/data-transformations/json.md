## JSON

Use Akka Stream JsonFraming to split a stream of @scaladoc[ByteString](akka.util.ByteString) elements into 
ByteString snippets of valid JSON objects. 

See @scaladoc[ScalaDSL JsonFraming](akka.stream.scaladsl.JsonFraming$) or @scaladoc[JavaDSL JsonFraming](akka.stream.javadsl.JsonFraming$)


@extref[Akka documentation](akka-docs:stream/stream-io.html#using-framing-in-your-protocol)



`JsonFraming.objectScanner(maximumObjectLength: Int): Flow[ByteString, ByteString, NotUsed]`

Returns a Flow that implements a "brace counting" based framing stage for emitting valid JSON chunks.

Typical examples of data that one may want to frame using this stage include:

**Very large arrays**:

[{"id": 1}, {"id": 2}, [...], {"id": 999}]

**Multiple concatenated JSON objects** (with, or without commas between them):

{"id": 1}, {"id": 2}, [...], {"id": 999}

The framing works independently of formatting, i.e. it will still emit valid JSON elements even if two 
elements are separated by multiple newlines or other whitespace characters. And of course is insensitive 
(and does not impact the emitting frame) to the JSON object's internal formatting. 