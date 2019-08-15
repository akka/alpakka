## Parsing Lines

Most Alpakka sources stream @scaladoc[ByteString](akka.util.ByteString) elements which normally won't contain data line by line. To 
split these at line separators use @scaladoc[ScalaDSL Framing](akka.stream.scaladsl.Framing$) or 
@scaladoc[JavaDSL Framing](akka.stream.javadsl.Framing$) as described in the Akka Stream documentation.

@extref:[Akka documentation](akka:stream/stream-cookbook.html#parsing-lines-from-a-stream-of-bytestrings)
