/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.http.javadsl.marshallers.jackson.Jackson;
import akka.http.javadsl.marshalling.Marshaller;
import akka.http.javadsl.model.HttpEntity;
import akka.http.javadsl.model.RequestEntity;
import akka.http.javadsl.unmarshalling.Unmarshaller;
import akka.stream.alpakka.googlecloud.bigquery.BigQueryAttributes;
import akka.stream.alpakka.googlecloud.bigquery.BigQuerySettings;
import akka.stream.alpakka.googlecloud.bigquery.RetryWithDeduplication;
import akka.stream.alpakka.googlecloud.bigquery.javadsl.BigQuery;
import akka.stream.alpakka.googlecloud.bigquery.javadsl.BigQueryCallbacks;
import akka.stream.alpakka.googlecloud.bigquery.javadsl.jackson.BigQueryMarshallers;
import akka.stream.alpakka.googlecloud.bigquery.model.DatasetJsonProtocol;
import akka.stream.alpakka.googlecloud.bigquery.model.JobJsonProtocol;
import akka.stream.alpakka.googlecloud.bigquery.model.QueryJsonProtocol;
import akka.stream.alpakka.googlecloud.bigquery.model.TableDataJsonProtocol;
import akka.stream.alpakka.googlecloud.bigquery.model.TableJsonProtocol;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;

public class BigQueryDoc {

  ActorSystem system = null;

  // #setup
  BigQuerySettings settings = BigQuery.getSettings(system);
  ObjectMapper objectMapper = new ObjectMapper();

  class Person {
    private String name;
    private Integer age;
    private List<Address> addresses;
    private Boolean isHakker;

    @JsonCreator
    public Person(@JsonProperty("f") JsonNode fields) throws IOException {
      name = fields.get(0).get("v").textValue();
      age = Integer.parseInt(fields.get(1).get("v").textValue());
      addresses = new ArrayList<>();
      for (JsonNode node : fields.get(2).get("v")) {
        Address address = objectMapper.readerFor(Address.class).readValue(node.get("v"));
        addresses.add(address);
      }
      isHakker = Boolean.parseBoolean(fields.get(3).get("v").textValue());
    }

    public String getName() {
      return name;
    }

    public Integer getAge() {
      return age;
    }

    public List<Address> getAddresses() {
      return addresses;
    }

    public Boolean getIsHakker() {
      return isHakker;
    }
  }

  class Address {
    private String street;
    private String city;
    private Integer postalCode;

    @JsonCreator
    public Address(@JsonProperty("f") JsonNode fields) {
      street = fields.get(0).get("v").textValue();
      city = fields.get(1).get("v").textValue();
      postalCode =
          Optional.of(fields.get(2).get("v").textValue()).map(Integer::parseInt).orElse(null);
    }

    public String getStreet() {
      return street;
    }

    public String getCity() {
      return city;
    }

    public Integer getPostalCode() {
      return postalCode;
    }
  }

  public class NameAddressesTuple {
    private String name;
    private List<Address> addresses;

    @JsonCreator
    public NameAddressesTuple(@JsonProperty("f") JsonNode fields) throws IOException {
      name = fields.get(0).get("v").textValue();
      addresses = new ArrayList<>();
      for (JsonNode node : fields.get(1).get("v")) {
        Address address = objectMapper.readerFor(Address.class).readValue(node.get("v"));
        addresses.add(address);
      }
    }
  }
  // #setup

  String datasetId;
  String tableId;

  void docs() {

    // #run-query
    String sqlQuery =
        String.format("SELECT name, addresses FROM %s.%s WHERE age > 100", datasetId, tableId);
    Unmarshaller<HttpEntity, QueryJsonProtocol.QueryResponse<NameAddressesTuple>>
        queryResponseUnmarshaller =
            BigQueryMarshallers.queryResponseUnmarshaller(NameAddressesTuple.class);
    Source<NameAddressesTuple, CompletionStage<QueryJsonProtocol.QueryResponse<NameAddressesTuple>>>
        centenarians =
            BigQuery.query(
                sqlQuery, false, false, BigQueryCallbacks.ignore(), queryResponseUnmarshaller);
    // #run-query

    // #dry-run-query
    Source<NameAddressesTuple, CompletionStage<QueryJsonProtocol.QueryResponse<NameAddressesTuple>>>
        centenariansDryRun =
            BigQuery.query(
                sqlQuery, false, false, BigQueryCallbacks.ignore(), queryResponseUnmarshaller);
    CompletionStage<OptionalLong> bytesProcessed =
        centenariansDryRun
            .to(Sink.ignore())
            .run(system)
            .thenApply(QueryJsonProtocol.QueryResponse::getTotalBytesProcessed);
    // #dry-run-query

    // #table-data
    Unmarshaller<HttpEntity, TableDataJsonProtocol.TableDataListResponse<Person>>
        tableDataListUnmarshaller =
            BigQueryMarshallers.tableDataListResponseUnmarshaller(Person.class);
    Source<Person, CompletionStage<TableDataJsonProtocol.TableDataListResponse<Person>>> everyone =
        BigQuery.listTableData(
            datasetId,
            tableId,
            OptionalLong.empty(),
            OptionalInt.empty(),
            Collections.emptyList(),
            tableDataListUnmarshaller);
    // #table-data

    // #streaming-insert
    Marshaller<TableDataJsonProtocol.TableDataInsertAllRequest<Person>, RequestEntity>
        tableDataInsertAllMarshaller = BigQueryMarshallers.tableDataListRequestMarshaller();
    Sink<List<Person>, NotUsed> peopleInsertSink =
        BigQuery.insertAll(
            datasetId,
            tableId,
            RetryWithDeduplication.getInstance(),
            Optional.empty(),
            tableDataInsertAllMarshaller);
    // #streaming-insert

    // #async-insert
    Flow<Person, JobJsonProtocol.Job, NotUsed> peopleLoadFlow =
        BigQuery.insertAllAsync(datasetId, tableId, Jackson.marshaller());
    // #async-insert

    List<Person> people = null;

    // #job-status
    Function<List<JobJsonProtocol.JobReference>, CompletionStage<Boolean>> checkIfJobsDone =
        jobReferences -> {
          CompletionStage<Boolean> allAreDone = CompletableFuture.completedFuture(true);
          for (JobJsonProtocol.JobReference jobReference : jobReferences) {
            CompletionStage<JobJsonProtocol.Job> job =
                BigQuery.getJob(jobReference.getJobId().get(), Optional.empty(), system, settings);
            CompletionStage<Boolean> jobIsDone =
                job.thenApply(
                    j ->
                        j.getStatus()
                            .map(s -> s.getState().equals(JobJsonProtocol.DoneState()))
                            .orElse(false));
            allAreDone = allAreDone.thenCombine(jobIsDone, (a, b) -> a & b);
          }
          return allAreDone;
        };

    CompletionStage<List<JobJsonProtocol.Job>> jobs =
        Source.from(people).via(peopleLoadFlow).runWith(Sink.<JobJsonProtocol.Job>seq(), system);
    CompletionStage<List<JobJsonProtocol.JobReference>> jobReferences =
        jobs.thenApply(
            js -> js.stream().map(j -> j.getJobReference().get()).collect(Collectors.toList()));
    CompletionStage<Boolean> isDone = jobReferences.thenCompose(checkIfJobsDone);
    // #job-status

    // #dataset-methods
    Source<DatasetJsonProtocol.Dataset, NotUsed> allDatasets =
        BigQuery.listDatasets(OptionalInt.empty(), Optional.empty(), Collections.emptyMap());
    CompletionStage<DatasetJsonProtocol.Dataset> existingDataset =
        BigQuery.getDataset(datasetId, system, settings);
    CompletionStage<DatasetJsonProtocol.Dataset> newDataset =
        BigQuery.createDataset("newDatasetId", system, settings);
    CompletionStage<Done> datasetDeleted =
        BigQuery.deleteDataset(datasetId, false, system, settings);
    // #dataset-methods

    // #table-methods
    Source<TableJsonProtocol.Table, CompletionStage<TableJsonProtocol.TableListResponse>>
        allTablesInDataset = BigQuery.listTables(datasetId, OptionalInt.empty());
    CompletionStage<TableJsonProtocol.Table> existingTable =
        BigQuery.getTable(datasetId, tableId, system, settings);
    CompletionStage<Done> tableDeleted = BigQuery.deleteTable(datasetId, tableId, system, settings);
    // #table-methods

    // #create-table
    TableJsonProtocol.TableSchema personSchema =
        TableJsonProtocol.createTableSchema(
            Arrays.asList(
                TableJsonProtocol.createTableFieldSchema(
                    "name", TableJsonProtocol.StringType(), Optional.empty(), Optional.empty()),
                TableJsonProtocol.createTableFieldSchema(
                    "age", TableJsonProtocol.IntegerType(), Optional.empty(), Optional.empty()),
                TableJsonProtocol.createTableFieldSchema(
                    "addresses",
                    TableJsonProtocol.RecordType(),
                    Optional.of(TableJsonProtocol.RepeatedMode()),
                    Optional.of(
                        Arrays.asList(
                            TableJsonProtocol.createTableFieldSchema(
                                "street",
                                TableJsonProtocol.StringType(),
                                Optional.empty(),
                                Optional.empty()),
                            TableJsonProtocol.createTableFieldSchema(
                                "city",
                                TableJsonProtocol.StringType(),
                                Optional.empty(),
                                Optional.empty()),
                            TableJsonProtocol.createTableFieldSchema(
                                "postalCode",
                                TableJsonProtocol.IntegerType(),
                                Optional.of(TableJsonProtocol.NullableMode()),
                                Optional.empty())))),
                TableJsonProtocol.createTableFieldSchema(
                    "isHakker",
                    TableJsonProtocol.BooleanType(),
                    Optional.empty(),
                    Optional.empty())));
    CompletionStage<TableJsonProtocol.Table> newTable =
        BigQuery.createTable(datasetId, "newTableId", personSchema, system, settings);
    // #create-table

    // #custom-settings
    BigQuerySettings defaultSettings = BigQuery.getSettings(system);
    BigQuerySettings customSettings = defaultSettings.withProjectId("myOtherProjectId");
    BigQuery.query(sqlQuery, false, false, BigQueryCallbacks.ignore(), queryResponseUnmarshaller)
        .withAttributes(BigQueryAttributes.settings(customSettings));
    // #custom-settings
  }
}
