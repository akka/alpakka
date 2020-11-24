/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

// #imports

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.http.javadsl.unmarshalling.Unmarshaller;
import akka.http.scaladsl.model.HttpRequest;
import akka.stream.ActorMaterializer;
import akka.stream.alpakka.googlecloud.bigquery.BigQueryConfig;
import akka.stream.alpakka.googlecloud.bigquery.client.BigQueryCommunicationHelper;
import akka.stream.alpakka.googlecloud.bigquery.client.TableDataQueryJsonProtocol;
import akka.stream.alpakka.googlecloud.bigquery.client.TableListQueryJsonProtocol;
import akka.stream.alpakka.googlecloud.bigquery.javadsl.BigQueryCallbacks;
import akka.stream.alpakka.googlecloud.bigquery.javadsl.GoogleBigQuerySource;
import akka.stream.alpakka.googlecloud.bigquery.javadsl.SprayJsonSupport;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import spray.json.JsObject;
import spray.json.JsValue;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
// #imports

public class GoogleBigQuerySourceDoc {

  private static void example() {
    // #init-mat
    ActorSystem system = ActorSystem.create();
    ActorMaterializer materializer = ActorMaterializer.create(system);
    // #init-mat

    // #init-config
    BigQueryConfig config =
        BigQueryConfig.create(
            "project@test.test",
            "privateKeyFromGoogle",
            "projectID",
            "bigQueryDatasetName",
            system);
    // #init-config

    // #list-tables-and-fields
    CompletionStage<List<TableListQueryJsonProtocol.QueryTableModel>> tables =
        GoogleBigQuerySource.listTables(config)
            .runWith(Sink.seq(), materializer)
            .thenApply(lt -> lt.stream().flatMap(Collection::stream).collect(Collectors.toList()));

    CompletionStage<List<TableDataQueryJsonProtocol.Field>> fields =
        GoogleBigQuerySource.listFields("myTable", config)
            .runWith(Sink.seq(), materializer)
            .thenApply(lt -> lt.stream().flatMap(Collection::stream).collect(Collectors.toList()));
    ;
    // #list-tables-and-fields

    // #csv-style
    Source<List<String>, NotUsed> userCsvLikeStream =
        GoogleBigQuerySource.runQueryCsvStyle(
            "SELECT uid, name FROM bigQueryDatasetName.myTable",
            BigQueryCallbacks.tryToStopJob(config, system, materializer),
            config);
    // #csv-style
  }

  // #run-query
  static class User {
    String uid;
    String name;

    User(String uid, String name) {
      this.uid = uid;
      this.name = name;
    }
  }

  static Unmarshaller<JsValue, User> userUnmarshaller =
      Unmarshaller.sync(GoogleBigQuerySourceDoc::userFromJson);

  static User userFromJson(JsValue value) {
    JsObject object = value.asJsObject();
    return new User(
        object.fields().apply("uid").toString(), object.fields().apply("name").toString());
  }

  private static Source<User, NotUsed> example2() {
    ActorSystem system = ActorSystem.create();
    ActorMaterializer materializer = ActorMaterializer.create(system);
    BigQueryConfig config =
        BigQueryConfig.create(
            "project@test.test",
            "privateKeyFromGoogle",
            "projectID",
            "bigQueryDatasetName",
            system);
    return GoogleBigQuerySource.runQuery(
        "SELECT uid, name FROM bigQueryDatasetName.myTable",
        userUnmarshaller,
        SprayJsonSupport.responseUnmarshaller(),
        SprayJsonSupport.jsValueUnmarshaller(),
        BigQueryCallbacks.ignore(),
        config);
  }
  // #run-query

  // #dry-run
  static class DryRunResponse {
    String totalBytesProcessed;
    String jobComplete;
    String cacheHit;

    DryRunResponse(String totalBytesProcessed, String jobComplete, String cacheHit) {
      this.totalBytesProcessed = totalBytesProcessed;
      this.jobComplete = jobComplete;
      this.cacheHit = cacheHit;
    }
  }

  static Unmarshaller<JsValue, DryRunResponse> dryRunResponseUnmarshaller =
      Unmarshaller.sync(GoogleBigQuerySourceDoc::dryRunResponseFromJson);

  static DryRunResponse dryRunResponseFromJson(JsValue value) {
    JsObject object = value.asJsObject();
    return new DryRunResponse(
        object.fields().apply("totalBytesProcessed").toString(),
        object.fields().apply("jobComplete").toString(),
        object.fields().apply("cacheHit").toString());
  }

  private static Source<DryRunResponse, NotUsed> example3() {
    ActorSystem system = ActorSystem.create();
    ActorMaterializer materializer = ActorMaterializer.create(system);
    BigQueryConfig config =
        BigQueryConfig.create(
            "project@test.test",
            "privateKeyFromGoogle",
            "projectID",
            "bigQueryDatasetName",
            system);

    HttpRequest request =
        BigQueryCommunicationHelper.createQueryRequest(
            "SELECT uid, name FROM bigQueryDatasetName.myTable", config.projectId(), true);

    return GoogleBigQuerySource.raw(
        request,
        dryRunResponseUnmarshaller,
        SprayJsonSupport.responseUnmarshaller(),
        SprayJsonSupport.jsValueUnmarshaller(),
        BigQueryCallbacks.ignore(),
        config);
  }
  // #dry-run
}
