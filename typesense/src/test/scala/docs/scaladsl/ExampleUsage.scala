/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.alpakka.typesense.scaladsl.Typesense
import akka.stream.alpakka.typesense._
import akka.stream.scaladsl.{Flow, Sink, Source}
import spray.json.RootJsonFormat

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

  final case class MyDocument(id: String, name: String)
  implicit val companyFormat: RootJsonFormat[MyDocument] = {
    import spray.json._
    import DefaultJsonProtocol._
    jsonFormat2(MyDocument)
  }

  //#index single document
  val indexSingleDocumentSource: Source[IndexDocument[MyDocument], NotUsed] =
    Source.single(IndexDocument("my-collection", MyDocument(UUID.randomUUID().toString, "Hello")))

  val indexSingleDocumentFlow: Flow[IndexDocument[MyDocument], Done, Future[NotUsed]] =
    Typesense.indexDocumentFlow(settings)

  val indexSingleDocumentResult: Future[Done] =
    indexSingleDocumentSource.via(indexSingleDocumentFlow).runWith(Sink.head)
  //#index single document

  //# retrieve document
  val retrieveDocumentSource: Source[RetrieveDocument, NotUsed] =
    Source.single(RetrieveDocument("my-collection", UUID.randomUUID().toString))

  val retrieveDocumentFlow: Flow[RetrieveDocument, MyDocument, Future[NotUsed]] =
    Typesense.retrieveDocumentFlow(settings)

  val retrieveDocumentResult: Future[MyDocument] =
    retrieveDocumentSource.via(retrieveDocumentFlow).runWith(Sink.head)
  //# retrieve document
}
