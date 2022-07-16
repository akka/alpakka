/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.alpakka.typesense.*;
import akka.stream.alpakka.typesense.javadsl.Typesense;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import spray.json.JsonReader;
import spray.json.JsonWriter;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

public class ExampleUsage {
  private static class MyDocument {
    final String id;
    final String name;

    MyDocument(String id, String name) {
      this.id = id;
      this.name = name;
    }
  }

  private static void example() {
    ActorSystem system = ActorSystem.create();

    // #settings
    String host = "http://localhost:8108";
    String apiKey = "Hu52dwsas2AdxdE";
    TypesenseSettings settings = TypesenseSettings.create(host, apiKey);
    // #setings

    // #create collection
    Field field = Field.create("name", FieldType.string());
    List<Field> fields = Collections.singletonList(field);
    CollectionSchema collectionSchema = CollectionSchema.create("my_collection", fields);

    Source<CollectionSchema, NotUsed> createCollectionSource = Source.single(collectionSchema);
    Flow<CollectionSchema, CollectionResponse, CompletionStage<NotUsed>> createCollectionFlow =
        Typesense.createCollectionFlow(settings);

    CompletionStage<CollectionResponse> createCollectionResponse =
        createCollectionSource.via(createCollectionFlow).runWith(Sink.head(), system);
    // #create collection

    // #retrieve collection
    Source<RetrieveCollection, NotUsed> retrieveCollectionSource =
        Source.single(RetrieveCollection.create("my-collection"));
    Flow<RetrieveCollection, CollectionResponse, CompletionStage<NotUsed>> retrieveCollectionFlow =
        Typesense.retrieveCollectionFlow(settings);

    CompletionStage<CollectionResponse> retrievedCollectionResponse =
        retrieveCollectionSource.via(retrieveCollectionFlow).runWith(Sink.head(), system);
    // #retrieve collection

    JsonWriter<MyDocument> documentJsonWriter = null;
    JsonReader<MyDocument> documentJsonReader = null;

    // #index single document
    Source<IndexDocument<MyDocument>, NotUsed> indexSingleDocumentSource =
        Source.single(
            IndexDocument.create(
                "my-collection", new MyDocument(UUID.randomUUID().toString(), "Hello")));

    Flow<IndexDocument<MyDocument>, Done, CompletionStage<NotUsed>> indexSingleDocumentFlow =
        Typesense.indexDocumentFlow(settings, documentJsonWriter);

    CompletionStage<Done> indexSingleDocumentResponse =
        indexSingleDocumentSource.via(indexSingleDocumentFlow).runWith(Sink.head(), system);
    // #index single document

    // #index many documents
    Source<IndexManyDocuments<MyDocument>, NotUsed> indexManyDocumentsSource =
        Source.single(
            IndexManyDocuments.create(
                "my-collection",
                Collections.singletonList(new MyDocument(UUID.randomUUID().toString(), "Hello"))));

    Flow<IndexManyDocuments<MyDocument>, List<IndexDocumentResult>, CompletionStage<NotUsed>>
        indexManyDocumentsFlow = Typesense.indexManyDocumentsFlow(settings, documentJsonWriter);

    CompletionStage<List<IndexDocumentResult>> indexManyDocumentsResult =
        indexManyDocumentsSource.via(indexManyDocumentsFlow).runWith(Sink.head(), system);
    // #index many documents

    // #retrieve document
    Source<RetrieveDocument, NotUsed> retrieveDocumentSource =
        Source.single(RetrieveDocument.create("my-collection", UUID.randomUUID().toString()));
    Flow<RetrieveDocument, MyDocument, CompletionStage<NotUsed>> retrieveDocumentFlow =
        Typesense.retrieveDocumentFlow(settings, documentJsonReader);

    CompletionStage<MyDocument> retrieveDocumentResponse =
        retrieveDocumentSource.via(retrieveDocumentFlow).runWith(Sink.head(), system);
    // #retrieve document
  }
}
