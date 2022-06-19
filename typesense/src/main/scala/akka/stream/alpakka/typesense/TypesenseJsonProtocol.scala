package akka.stream.alpakka.typesense

import spray.json.DefaultJsonProtocol

private[typesense] object TypesenseJsonProtocol extends DefaultJsonProtocol {
  import spray.json._
  implicit val fieldFormat: RootJsonFormat[Field] = jsonFormat2(Field)
  implicit val collectionSchemaFormat: RootJsonFormat[CollectionSchema] = jsonFormat2(CollectionSchema)
  implicit val collectionResponseFormat: RootJsonFormat[CollectionResponse] =
    jsonFormat(CollectionResponse, "name", "num_documents", "fields")
}
