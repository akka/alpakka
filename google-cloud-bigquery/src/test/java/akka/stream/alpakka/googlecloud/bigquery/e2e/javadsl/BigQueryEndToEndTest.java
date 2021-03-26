/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.e2e.javadsl;

import akka.Done;
import akka.actor.ActorSystem;
import akka.http.javadsl.marshallers.jackson.Jackson;
import akka.japi.tuple.Tuple3;
import akka.stream.alpakka.google.GoogleSettings;
import akka.stream.alpakka.googlecloud.bigquery.BigQueryHoverfly;
import akka.stream.alpakka.googlecloud.bigquery.e2e.A;
import akka.stream.alpakka.googlecloud.bigquery.e2e.B;
import akka.stream.alpakka.googlecloud.bigquery.javadsl.BigQuery;
import akka.stream.alpakka.googlecloud.bigquery.javadsl.jackson.BigQueryMarshallers;
import akka.stream.alpakka.googlecloud.bigquery.model.Dataset;
import akka.stream.alpakka.googlecloud.bigquery.model.Job;
import akka.stream.alpakka.googlecloud.bigquery.model.JobReference;
import akka.stream.alpakka.googlecloud.bigquery.model.JobState;
import akka.stream.alpakka.googlecloud.bigquery.model.JobStatus;
import akka.stream.alpakka.googlecloud.bigquery.model.QueryResponse;
import akka.stream.alpakka.googlecloud.bigquery.model.Table;
import akka.stream.alpakka.googlecloud.bigquery.model.TableFieldSchema;
import akka.stream.alpakka.googlecloud.bigquery.model.TableFieldSchemaMode;
import akka.stream.alpakka.googlecloud.bigquery.model.TableFieldSchemaType;
import akka.stream.alpakka.googlecloud.bigquery.model.TableSchema;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.specto.hoverfly.junit.core.Hoverfly;
import io.specto.hoverfly.junit.core.HoverflyMode;
import io.specto.hoverfly.junit.core.SimulationSource;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class BigQueryEndToEndTest extends EndToEndHelper {

  private static ActorSystem system = ActorSystem.create("BigQueryEndToEndTest");
  private static Hoverfly hoverfly = BigQueryHoverfly.getInstance();

  private GoogleSettings settings = GoogleSettings.create(system);
  private ObjectMapper objectMapper =
      JsonMapper.builder()
          .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
          .addModule(new JavaTimeModule())
          .build();

  private TableSchema schema =
      TableSchema.create(
          TableFieldSchema.create(
              "integer",
              TableFieldSchemaType.integer(),
              Optional.of(TableFieldSchemaMode.required())),
          TableFieldSchema.create(
              "long", TableFieldSchemaType.integer(), Optional.of(TableFieldSchemaMode.required())),
          TableFieldSchema.create(
              "float",
              TableFieldSchemaType.float64(),
              Optional.of(TableFieldSchemaMode.required())),
          TableFieldSchema.create(
              "double",
              TableFieldSchemaType.float64(),
              Optional.of(TableFieldSchemaMode.required())),
          TableFieldSchema.create(
              "string",
              TableFieldSchemaType.string(),
              Optional.of(TableFieldSchemaMode.required())),
          TableFieldSchema.create(
              "boolean", TableFieldSchemaType.bool(), Optional.of(TableFieldSchemaMode.required())),
          TableFieldSchema.create(
              "record",
              TableFieldSchemaType.record(),
              Optional.of(TableFieldSchemaMode.required()),
              TableFieldSchema.create(
                  "nullable",
                  TableFieldSchemaType.string(),
                  Optional.of(TableFieldSchemaMode.nullable())),
              TableFieldSchema.create(
                  "bytes",
                  TableFieldSchemaType.bytes(),
                  Optional.of(TableFieldSchemaMode.required())),
              TableFieldSchema.create(
                  "repeated",
                  TableFieldSchemaType.record(),
                  Optional.of(TableFieldSchemaMode.repeated()),
                  TableFieldSchema.create(
                      "numeric",
                      TableFieldSchemaType.numeric(),
                      Optional.of(TableFieldSchemaMode.required())),
                  TableFieldSchema.create(
                      "date",
                      TableFieldSchemaType.date(),
                      Optional.of(TableFieldSchemaMode.required())),
                  TableFieldSchema.create(
                      "time",
                      TableFieldSchemaType.time(),
                      Optional.of(TableFieldSchemaMode.required())),
                  TableFieldSchema.create(
                      "dateTime",
                      TableFieldSchemaType.dateTime(),
                      Optional.of(TableFieldSchemaMode.required())),
                  TableFieldSchema.create(
                      "timestamp",
                      TableFieldSchemaType.timestamp(),
                      Optional.of(TableFieldSchemaMode.required())))));

  @BeforeClass
  public static void before() {
    hoverfly.start();
    switch (system.settings().config().getString("alpakka.google.bigquery.test.e2e-mode")) {
      case "simulate":
        hoverfly.simulate(
            SimulationSource.url(
                BigQueryEndToEndTest.class
                    .getClassLoader()
                    .getResource("BigQueryEndToEndSpec.json")));
        break;
      case "capture":
        hoverfly.resetMode(HoverflyMode.CAPTURE);
        break;
      default:
        throw new IllegalArgumentException();
    }
  }

  @AfterClass
  public static void after() {
    hoverfly.close();
    system.terminate();
  }

  @Test
  public void createDataset() throws ExecutionException, InterruptedException {
    Dataset dataset =
        BigQuery.createDataset(datasetId(), settings, system).toCompletableFuture().get();
    assertEquals(getDatasetId(), dataset.getDatasetReference().getDatasetId().get());
  }

  @Test
  public void listNewDataset() throws ExecutionException, InterruptedException {
    List<Dataset> datasets =
        BigQuery.listDatasets(OptionalInt.empty(), Optional.empty(), Collections.emptyMap())
            .runWith(Sink.seq(), system)
            .toCompletableFuture()
            .get();
    assertTrue(
        datasets.stream()
            .anyMatch(
                dataset ->
                    dataset.getDatasetReference().getDatasetId().get().equals(getDatasetId())));
  }

  @Test
  public void createTable() throws ExecutionException, InterruptedException {
    Table table =
        BigQuery.createTable(datasetId(), tableId(), schema, settings, system)
            .toCompletableFuture()
            .get();
    assertEquals(getTableId(), table.getTableReference().getTableId().get());
  }

  @Test
  public void listNewTable() throws ExecutionException, InterruptedException {
    List<Table> tables =
        BigQuery.listTables(datasetId(), OptionalInt.empty())
            .runWith(Sink.seq(), system)
            .toCompletableFuture()
            .get();
    assertTrue(
        tables.stream()
            .anyMatch(table -> table.getTableReference().getTableId().get().equals(getTableId())));
  }

  private CompletionStage<Job> waitUntilJobComplete(Job job) {
    return BigQuery.getJob(
            job.getJobReference().flatMap(JobReference::getJobId).get(),
            Optional.empty(),
            settings,
            system)
        .thenComposeAsync(
            job2 -> {
              if (job2.getStatus()
                  .filter(status -> status.getState().equals(JobState.done()))
                  .isPresent()) {
                return CompletableFuture.completedFuture(job2);
              } else {
                try {
                  Thread.sleep((hoverfly.getMode() == HoverflyMode.SIMULATE) ? 1000 : 0);
                  return waitUntilJobComplete(job2);
                } catch (Exception ex) {
                  throw new RuntimeException(ex);
                }
              }
            });
  }

  @Test
  public void insertRowsViaLoadJobs() throws ExecutionException, InterruptedException {
    List<Job> jobs =
        Source.from(getRows())
            .via(BigQuery.insertAllAsync(datasetId(), tableId(), Jackson.marshaller(objectMapper)))
            .runWith(Sink.seq(), system)
            .toCompletableFuture()
            .get();
    assertEquals(1, jobs.size());
    Job job = waitUntilJobComplete(jobs.get(0)).toCompletableFuture().get();
    assertFalse(job.getStatus().flatMap(JobStatus::getErrorResult).isPresent());
  }

  private <T> List<T> sorted(List<T> list) {
    return list.stream().sorted(Comparator.comparingInt(T::hashCode)).collect(Collectors.toList());
  }

  @Test
  public void retrieveRows() throws ExecutionException, InterruptedException {
    List<A> rows =
        BigQuery.listTableData(
                datasetId(),
                tableId(),
                OptionalLong.empty(),
                OptionalInt.empty(),
                Collections.emptyList(),
                BigQueryMarshallers.tableDataListResponseUnmarshaller(A.class))
            .runWith(Sink.seq(), system)
            .toCompletableFuture()
            .get();
    assertEquals(sorted(getRows()), sorted(rows));
  }

  @Test
  public void runQuery() throws ExecutionException, InterruptedException {
    String query =
        String.format(
            "SELECT string, record, integer FROM %s.%s WHERE boolean;", datasetId(), tableId());
    List<Tuple3<String, B, Integer>> expectedResults =
        getRows().stream()
            .filter(A::getBoolean)
            .map(a -> new Tuple3<>(a.getString(), a.getRecord(), a.getInteger()))
            .collect(Collectors.toList());
    List<Tuple3<String, B, Integer>> results =
        BigQuery.query(
                query, false, false, BigQueryMarshallers.queryResponseUnmarshaller(JsonNode.class))
            .map(
                node ->
                    new Tuple3<>(
                        node.get("f").get(0).get("v").textValue(),
                        new B(node.get("f").get(1).get("v")),
                        Integer.parseInt(node.get("f").get(2).get("v").textValue())))
            .runWith(Sink.seq(), system)
            .toCompletableFuture()
            .get();
    assertEquals(sorted(expectedResults), sorted(results));
  }

  @Test
  public void dryRunQuery() throws ExecutionException, InterruptedException {
    String query =
        String.format(
            "SELECT string, record, integer FROM %s.%s WHERE boolean;", datasetId(), tableId());
    QueryResponse<JsonNode> response =
        BigQuery.query(
                query, true, false, BigQueryMarshallers.queryResponseUnmarshaller(JsonNode.class))
            .to(Sink.ignore())
            .run(system)
            .toCompletableFuture()
            .get();
    OptionalLong bytesProcessed = response.getTotalBytesProcessed();
    assertTrue(bytesProcessed.isPresent() && bytesProcessed.getAsLong() > 0);
  }

  @Test
  public void deleteTable() throws ExecutionException, InterruptedException {
    Done done =
        BigQuery.deleteTable(datasetId(), tableId(), settings, system).toCompletableFuture().get();
    assertEquals(Done.done(), done);
  }

  @Test
  public void notListDeletedTable() throws ExecutionException, InterruptedException {
    List<Table> tables =
        BigQuery.listTables(datasetId(), OptionalInt.empty())
            .runWith(Sink.seq(), system)
            .toCompletableFuture()
            .get();
    assertTrue(
        tables.stream()
            .noneMatch(table -> table.getTableReference().getTableId().equals(getTableId())));
  }

  @Test
  public void deleteDataset() throws ExecutionException, InterruptedException {
    Done done =
        BigQuery.deleteDataset(datasetId(), false, settings, system).toCompletableFuture().get();
    assertEquals(Done.done(), done);
  }

  @Test
  public void notListDeletedDataset() throws ExecutionException, InterruptedException {
    List<Dataset> datasets =
        BigQuery.listDatasets(OptionalInt.empty(), Optional.empty(), Collections.emptyMap())
            .runWith(Sink.seq(), system)
            .toCompletableFuture()
            .get();
    assertTrue(
        datasets.stream()
            .noneMatch(
                dataset -> dataset.getDatasetReference().getDatasetId().equals(getDatasetId())));
  }
}
