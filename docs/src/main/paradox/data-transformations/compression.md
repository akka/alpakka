## Compressing/decompressing

Akka Stream contains support for compressing and decompressing streams of @apidoc[ByteString](akka.util.ByteString)
elements.

Use @apidoc[Compression](Compression$) as described in the Akka Stream documentation:

@extref:[Akka documentation](akka:stream/stream-cookbook.html#dealing-with-compressed-data-streams)

### ZIP

@ref[Alpakka File](../file.md#zip-archive) supports creating flows in ZIP-format.
