/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.alpakka.typesense.scaladsl.{FilterDeleteDocumentsQueryDsl, Typesense}
import akka.stream.alpakka.typesense._
import akka.stream.scaladsl.{Flow, Sink, Source}
import spray.json.RootJsonFormat

import java.util.UUID
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class ExampleUsage {
  import ExampleUsage._
  implicit val system: ActorSystem = ActorSystem()

  //#settings
  val host: String = "http://localhost:8108"
  val apiKey: String = "Hu52dwsas2AdxdE"
  val retrySettings: RetrySettings =
    RetrySettings(maxRetries = 6, minBackoff = 1.second, maxBackoff = 1.minute, randomFactor = 0.2)
  val settings: TypesenseSettings = TypesenseSettings(host, apiKey, retrySettings)
  //#setings

  //#create collection
  val field: Field = Field("name", FieldType.String)
  val fields: Seq[Field] = Seq(field)
  val collectionSchema: CollectionSchema = CollectionSchema("my-collection", fields)

  val createCollectionSource: Source[CollectionSchema, NotUsed] = Source.single(collectionSchema)
  val createCollectionFlow: Flow[CollectionSchema, TypesenseResult[CollectionResponse], Future[NotUsed]] =
    Typesense.createCollectionFlow(settings)

  val createdCollectionResponse: Future[TypesenseResult[CollectionResponse]] =
    createCollectionSource.via(createCollectionFlow).runWith(Sink.head)
  //#create collection

  //#retrieve collection
  val retrieveCollectionSource: Source[RetrieveCollection, NotUsed] = Source.single(RetrieveCollection("my-collection"))
  val retrieveCollectionFlow: Flow[RetrieveCollection, TypesenseResult[CollectionResponse], Future[NotUsed]] =
    Typesense.retrieveCollectionFlow(settings)

  val retrievedCollectionResponse: Future[TypesenseResult[CollectionResponse]] =
    retrieveCollectionSource.via(retrieveCollectionFlow).runWith(Sink.head)
  //#retrieve collection

  //#index single document
  val indexSingleDocumentSource: Source[IndexDocument[MyDocument], NotUsed] =
    Source.single(IndexDocument("my-collection", MyDocument(UUID.randomUUID().toString, "Hello")))

  val indexSingleDocumentFlow: Flow[IndexDocument[MyDocument], TypesenseResult[Done], Future[NotUsed]] =
    Typesense.indexDocumentFlow(settings)

  val indexSingleDocumentResult: Future[TypesenseResult[Done]] =
    indexSingleDocumentSource.via(indexSingleDocumentFlow).runWith(Sink.head)
  //#index single document

  //#index many documents
  val indexManyDocumentsSource: Source[IndexManyDocuments[MyDocument], NotUsed] =
    Source.single(IndexManyDocuments("my-collection", Seq(MyDocument(UUID.randomUUID().toString, "Hello"))))

  val indexManyDocumentsFlow: Flow[IndexManyDocuments[MyDocument], Seq[IndexDocumentResult], Future[NotUsed]] =
    Typesense.indexManyDocumentsFlow(settings)

  val indexManyDocumentsResult: Future[Seq[IndexDocumentResult]] =
    indexManyDocumentsSource.via(indexManyDocumentsFlow).runWith(Sink.head)
  //#index many documents

  //# retrieve document
  val retrieveDocumentSource: Source[RetrieveDocument, NotUsed] =
    Source.single(RetrieveDocument("my-collection", UUID.randomUUID().toString))

  val retrieveDocumentFlow: Flow[RetrieveDocument, TypesenseResult[MyDocument], Future[NotUsed]] =
    Typesense.retrieveDocumentFlow(settings)

  val retrieveDocumentResult: Future[TypesenseResult[MyDocument]] =
    retrieveDocumentSource.via(retrieveDocumentFlow).runWith(Sink.head)
  //# retrieve document

  //# delete document
  val deleteDocumentSource: Source[DeleteDocument, NotUsed] =
    Source.single(DeleteDocument("my-collection", UUID.randomUUID().toString))

  val deleteDocumentFlow: Flow[DeleteDocument, TypesenseResult[Done], Future[NotUsed]] =
    Typesense.deleteDocumentFlow(settings)

  val deleteDocumentResult: Future[TypesenseResult[Done]] =
    deleteDocumentSource.via(deleteDocumentFlow).runWith(Sink.head)
  //# delete document

  //# delete documents by query
  val deleteDocumentsByQuerySource: Source[DeleteManyDocumentsByQuery, NotUsed] =
    Source.single(DeleteManyDocumentsByQuery("my-collection", "budget:>150"))

  val deleteDocumentsByQueryWithDslSource: Source[DeleteManyDocumentsByQuery, NotUsed] =
    Source.single(
      DeleteManyDocumentsByQuery("my-collection",
                                 FilterDeleteDocumentsQueryDsl.inStringSet("my-field", Seq(UUID.randomUUID().toString)))
    )

  val deleteDocumentsByQueryFlow: Flow[DeleteManyDocumentsByQuery, DeleteManyDocumentsResult, Future[NotUsed]] =
    Typesense.deleteManyDocumentsByQueryFlow(settings)

  val deleteDocumentsByQueryResult: Future[DeleteManyDocumentsResult] =
    deleteDocumentsByQuerySource.via(deleteDocumentsByQueryFlow).runWith(Sink.head)
  //# delete documents by query
}

object ExampleUsage {
  final case class MyDocument(id: String, name: String)
  implicit val companyFormat: RootJsonFormat[MyDocument] = {
    import spray.json._
    import DefaultJsonProtocol._
    jsonFormat2(MyDocument)
  }
}
