package akka.stream.alpakka.typesense

//TODO: add tokenSeparators, symbolsToIndex, defaultSortingField)
final case class CollectionSchema(name: String, fields: Seq[Field])

//TODO: add tokenSeparators, symbolsToIndex, defaultSortingField
final case class CollectionResponse(name: String, numDocuments: Int, fields: Seq[Field])

//TODO: add flags optional, facet, index
//TODO: field type enum //https://typesense.org/docs/0.23.0/api/collections.html#schema-parameters
final case class Field(name: String, `type`: String)
