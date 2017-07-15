## Parsing Lines

Most Alpakka sources stream @scaladoc[ByteString](akka.util.ByteString) elements which normally won't contain data line by line. To 
split these at line separators use @scaladoc[ScalaDSL Framing](akka.stream.scaladsl.Framing$) or 
@scaladoc[JavaDSL Framing](akka.stream.javadsl.Framing$) as described in the Akka Stream documentation.

@extref[Java documentation](akka-docs:java/stream/stream-cookbook.html#Parsing_lines_from_a_stream_of_ByteStrings)

@extref[Scala documentation](akka-docs:scala/stream/stream-cookbook.html#Parsing_lines_from_a_stream_of_ByteStrings)

