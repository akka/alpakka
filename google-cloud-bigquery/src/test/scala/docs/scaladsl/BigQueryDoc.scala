/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

//#imports
import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.stream.alpakka.googlecloud.bigquery.model.DatasetJsonProtocol.Dataset
import akka.stream.alpakka.googlecloud.bigquery.model.JobJsonProtocol
import akka.stream.alpakka.googlecloud.bigquery.model.JobJsonProtocol.{Job, JobReference}
import akka.stream.alpakka.googlecloud.bigquery.model.QueryJsonProtocol.QueryResponse
import akka.stream.alpakka.googlecloud.bigquery.model.TableDataJsonProtocol.TableDataListResponse
import akka.stream.alpakka.googlecloud.bigquery.model.TableJsonProtocol.{Table, TableListResponse}
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.BigQuery
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.schema.BigQuerySchemas._
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray.BigQueryJsonProtocol._
import akka.stream.alpakka.googlecloud.bigquery.{BigQueryAttributes, BigQuerySettings, InsertAllRetryPolicy}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.{Done, NotUsed}

import scala.collection.immutable.Seq
import scala.concurrent.Future
//#imports

class BigQueryDoc {

  implicit val system: ActorSystem = ???
  import system.dispatcher

  //#setup
  case class Person(name: String, age: Int, addresses: Seq[Address], isHakker: Boolean)
  case class Address(street: String, city: String, postalCode: Option[Int])
  implicit val addressFormat = bigQueryJsonFormat3(Address)
  implicit val personFormat = bigQueryJsonFormat4(Person)
  //#setup

  val datasetId: String = ???
  val tableId: String = ???

  //#run-query
  val sqlQuery = s"SELECT name, addresses FROM $datasetId.$tableId WHERE age >= 100"
  val centenarians: Source[(String, Seq[Address]), Future[QueryResponse[(String, Seq[Address])]]] =
    BigQuery.query[(String, Seq[Address])](sqlQuery, useLegacySql = false)
  //#run-query

  //#dry-run-query
  val centenariansDryRun = BigQuery.query[(String, Seq[Address])](sqlQuery, dryRun = true, useLegacySql = false)
  val bytesProcessed: Future[Long] = centenariansDryRun.to(Sink.ignore).run().map(_.totalBytesProcessed.get)
  //#dry-run-query

  //#table-data
  val everyone: Source[Person, Future[TableDataListResponse[Person]]] =
    BigQuery.tableData[Person](datasetId, tableId)
  //#table-data

  //#streaming-insert
  val peopleInsertSink: Sink[Seq[Person], NotUsed] =
    BigQuery.insertAll[Person](datasetId, tableId, InsertAllRetryPolicy.WithDeduplication)
  //#streaming-insert

  //#async-insert
  val peopleLoadFlow: Flow[Person, Job, NotUsed] = BigQuery.insertAllAsync[Person](datasetId, tableId)
  //#async-insert

  val people: List[Person] = ???

  //#job-status
  def checkIfJobsDone(jobReferences: Seq[JobReference]): Future[Boolean] = {
    for {
      jobs <- Future.sequence(jobReferences.map(ref => BigQuery.job(ref.jobId.get)))
    } yield jobs.forall(job => job.status.exists(_.state == JobJsonProtocol.DoneState))
  }

  val isDone: Future[Boolean] = for {
    jobs <- Source(people).via(peopleLoadFlow).runWith(Sink.seq)
    jobReferences = jobs.flatMap(job => job.jobReference)
    isDone <- checkIfJobsDone(jobReferences)
  } yield isDone
  //#job-status

  //#dataset-methods
  val allDatasets: Source[Dataset, NotUsed] = BigQuery.datasets
  val existingDataset: Future[Dataset] = BigQuery.dataset(datasetId)
  val newDataset: Future[Dataset] = BigQuery.createDataset("newDatasetId")
  val datasetDeleted: Future[Done] = BigQuery.deleteDataset(datasetId)
  //#dataset-methods

  //#table-methods
  val allTablesInDataset: Source[Table, Future[TableListResponse]] = BigQuery.tables(datasetId)
  val existingTable: Future[Table] = BigQuery.table(datasetId, tableId)
  val tableDeleted: Future[Done] = BigQuery.deleteTable(datasetId, tableId)
  //#table-methods

  //#create-table
  implicit val addressSchema = bigQuerySchema3(Address)
  implicit val personSchema = bigQuerySchema4(Person)
  val newTable: Future[Table] = BigQuery.createTable[Person](datasetId, "newTableId")
  //#create-table

  //#custom-settings
  val defaultSettings: BigQuerySettings = BigQuery.settings
  val customSettings = defaultSettings.copy(projectId = "myOtherProject")
  BigQuery.query[(String, Seq[Address])](sqlQuery).withAttributes(BigQueryAttributes.settings(customSettings))
  //#custom-settings

}
