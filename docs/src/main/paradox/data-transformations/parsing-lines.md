## Parsing Lines

Most Alpakka sources stream @apidoc[akka.util.ByteString] elements which normally won't contain data line by line. To 
split these at line separators use @apidoc[Framing$] as described in the Akka Stream documentation.

@extref:[Akka documentation](akka:stream/stream-cookbook.html#parsing-lines-from-a-stream-of-bytestrings)
