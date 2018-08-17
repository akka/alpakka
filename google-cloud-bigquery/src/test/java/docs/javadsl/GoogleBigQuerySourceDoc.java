/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

// #imports
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.http.scaladsl.model.HttpRequest;
import akka.stream.ActorMaterializer;
import akka.stream.alpakka.google.cloud.bigquery.BigQueryCommunicationHelper;
import akka.stream.alpakka.google.cloud.bigquery.BigQueryFlowModels;
import akka.stream.alpakka.google.cloud.bigquery.impl.client.TableDataQueryJsonProtocol;
import akka.stream.alpakka.google.cloud.bigquery.impl.client.TableListQueryJsonProtocol;
import akka.stream.alpakka.google.cloud.bigquery.javadsl.GoogleBigQuerySource;
import akka.stream.javadsl.Source;
import spray.json.JsObject;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
// #imports

public class GoogleBigQuerySourceDoc {

  private static void example() {
    // #init-mat
    ActorSystem system = ActorSystem.create();
    ActorMaterializer materializer = ActorMaterializer.create(system);
    // #init-mat

    // #init-config
    BigQueryFlowModels.BigQueryProjectConfig config =
        GoogleBigQuerySource.createProjectConfig(
            "project@test.test",
            "privateKeyFromGoogle",
            "projectID",
            "bigQueryDatasetName",
            system);
    // #init-config

    // #list-tables-and-fields
    CompletionStage<List<TableListQueryJsonProtocol.QueryTableModel>> tables =
        GoogleBigQuerySource.listTables(config, materializer, system);
    CompletionStage<List<TableDataQueryJsonProtocol.Field>> fields =
        GoogleBigQuerySource.listFields("myTable", config, materializer, system);
    // #list-tables-and-fields

    // #csv-style
    Source<List<String>, NotUsed> userCsvLikeStream =
        GoogleBigQuerySource.runQueryCsvStyle(
            "SELECT uid, name FROM bigQueryDatasetName.myTable", config, materializer, system);
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

  static Optional<User> userFromJson(JsObject object) {
    try {
      return Optional.of(
          new User(
              object.fields().apply("uid").toString(), object.fields().apply("name").toString()));
    } catch (Throwable ex) {
      return Optional.empty();
    }
  }

  private static Source<User, NotUsed> example2() {
    ActorSystem system = ActorSystem.create();
    ActorMaterializer materializer = ActorMaterializer.create(system);
    BigQueryFlowModels.BigQueryProjectConfig config =
        GoogleBigQuerySource.createProjectConfig(
            "project@test.test",
            "privateKeyFromGoogle",
            "projectID",
            "bigQueryDatasetName",
            system);
    return GoogleBigQuerySource.runQuery(
        "SELECT uid, name FROM bigQueryDatasetName.myTable",
        GoogleBigQuerySourceDoc::userFromJson,
        config,
        materializer,
        system);
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

  static Optional<DryRunResponse> dryRunResponseFromJson(JsObject object) {
    try {
      return Optional.of(
          new DryRunResponse(
              object.fields().apply("totalBytesProcessed").toString(),
              object.fields().apply("jobComplete").toString(),
              object.fields().apply("cacheHit").toString()));
    } catch (Throwable ex) {
      return Optional.empty();
    }
  }

  private static Source<DryRunResponse, NotUsed> example3() {
    ActorSystem system = ActorSystem.create();
    ActorMaterializer materializer = ActorMaterializer.create(system);
    BigQueryFlowModels.BigQueryProjectConfig config =
        GoogleBigQuerySource.createProjectConfig(
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
        GoogleBigQuerySourceDoc::dryRunResponseFromJson,
        config.session(),
        materializer,
        system);
  }
  // #dry-run
}
