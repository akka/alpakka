package docs.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.alpakka.typesense.*;
import akka.stream.alpakka.typesense.javadsl.Typesense;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;

public class ExampleUsage {
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
  }
}
