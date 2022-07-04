package docs.scaladsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.alpakka.typesense.scaladsl.Typesense
import akka.stream.alpakka.typesense.{
  CollectionResponse,
  CollectionSchema,
  Field,
  FieldType,
  RetrieveCollection,
  TypesenseSettings
}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}

import java.util.UUID
import scala.concurrent.Future

class ExampleUsage {
  implicit val system: ActorSystem = ActorSystem()

  //#settings
  val host: String = "http://localhost:8108"
  val apiKey: String = "Hu52dwsas2AdxdE"
  val settings: TypesenseSettings = TypesenseSettings(host, apiKey)
  //#setings

  //#create collection
  val field: Field = Field("name", FieldType.String)
  val fields: Seq[Field] = Seq(field)
  val collectionSchema: CollectionSchema = CollectionSchema("my-collection", fields)

  val createCollectionSource: Source[CollectionSchema, NotUsed] = Source.single(collectionSchema)
  val createCollectionFlow: Flow[CollectionSchema, CollectionResponse, Future[NotUsed]] =
    Typesense.createCollectionFlow(settings)

  val createdCollectionResponse: Future[CollectionResponse] =
    createCollectionSource.via(createCollectionFlow).runWith(Sink.head)
  //#create collection

  //#retrieve collection
  val retrieveCollectionSource: Source[RetrieveCollection, NotUsed] = Source.single(RetrieveCollection("my-collection"))
  val retrieveCollectionFlow: Flow[RetrieveCollection, CollectionResponse, Future[NotUsed]] =
    Typesense.retrieveCollectionFlow(settings)

  val retrievedCollectionResponse: Future[CollectionResponse] =
    retrieveCollectionSource.via(retrieveCollectionFlow).runWith(Sink.head)
  //#retrieve collection
}
