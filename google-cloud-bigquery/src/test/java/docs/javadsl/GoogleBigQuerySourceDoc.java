/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

// #imports

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.http.scaladsl.model.HttpRequest;
import akka.stream.Materializer;
import akka.stream.alpakka.googlecloud.bigquery.BigQueryConfig;
import akka.stream.alpakka.googlecloud.bigquery.client.BigQueryCommunicationHelper;
import akka.stream.alpakka.googlecloud.bigquery.client.TableDataQueryJsonProtocol;
import akka.stream.alpakka.googlecloud.bigquery.client.TableListQueryJsonProtocol;
import akka.stream.alpakka.googlecloud.bigquery.javadsl.BigQueryCallbacks;
import akka.stream.alpakka.googlecloud.bigquery.javadsl.GoogleBigQuerySource;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import scala.util.Try;
import spray.json.JsArray;
import spray.json.JsObject;
import spray.json.JsString;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
// #imports

public class GoogleBigQuerySourceDoc {

  private static void example() {
    // #init-mat
    ActorSystem system = ActorSystem.create();
    Materializer materializer = Materializer.createMaterializer(system);
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

  static Try<User> userFromJson(JsObject object) {
    return Try.apply(
        () -> {
          JsArray f = (JsArray) object.fields().apply("f");
          String uid = ((JsString) f.elements().apply(0).asJsObject().fields().apply("v")).value();
          String name = ((JsString) f.elements().apply(1).asJsObject().fields().apply("v")).value();
          return new User(uid, name);
        });
  }

  private static Source<User, NotUsed> example2() {
    ActorSystem system = ActorSystem.create();
    Materializer materializer = Materializer.createMaterializer(system);
    BigQueryConfig config =
        BigQueryConfig.create(
            "project@test.test",
            "privateKeyFromGoogle",
            "projectID",
            "bigQueryDatasetName",
            system);
    return GoogleBigQuerySource.runQuery(
        "SELECT uid, name FROM bigQueryDatasetName.myTable",
        GoogleBigQuerySourceDoc::userFromJson,
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

  static Try<DryRunResponse> dryRunResponseFromJson(JsObject object) {
    scala.Function0<DryRunResponse> responseFunction =
        () ->
            new DryRunResponse(
                object.fields().apply("totalBytesProcessed").toString(),
                object.fields().apply("jobComplete").toString(),
                object.fields().apply("cacheHit").toString());
    return Try.apply(responseFunction);
  }

  private static Source<DryRunResponse, NotUsed> example3() {
    ActorSystem system = ActorSystem.create();
    Materializer materializer = Materializer.createMaterializer(system);
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
        GoogleBigQuerySourceDoc::dryRunResponseFromJson,
        BigQueryCallbacks.ignore(),
        config);
  }
  // #dry-run
}
