/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.model

import akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray.BigQueryRestJsonProtocol._
import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import com.github.ghik.silencer.silent
import spray.json.{JsonFormat, RootJsonFormat}

import java.util
import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.compat.java8.OptionConverters._

/**
 * Job model
 * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/Job BigQuery reference]]
 *
 * @param configuration describes the job configuration
 * @param jobReference reference describing the unique-per-user name of the job
 * @param status the status of this job
 */
final case class Job(configuration: Option[JobConfiguration],
                     jobReference: Option[JobReference],
                     status: Option[JobStatus]) {

  def getConfiguration = configuration.asJava
  def getJobReference = jobReference.asJava
  def getStatus = status.asJava

  def withConfiguration(configuration: Option[JobConfiguration]) =
    copy(configuration = configuration)
  def withConfiguration(configuration: util.Optional[JobConfiguration]) =
    copy(configuration = configuration.asScala)

  def withJobReference(jobReference: Option[JobReference]) =
    copy(jobReference = jobReference)
  def withJobReference(jobReference: util.Optional[JobReference]) =
    copy(jobReference = jobReference.asScala)

  def withStatus(status: Option[JobStatus]) =
    copy(status = status)
  def withStatus(status: util.Optional[JobStatus]) =
    copy(status = status.asScala)
}

object Job {

  /**
   * Java API: Job model
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/Job BigQuery reference]]
   *
   * @param configuration describes the job configuration
   * @param jobReference reference describing the unique-per-user name of the job
   * @param status the status of this job
   * @return a [[Job]]
   */
  def create(configuration: util.Optional[JobConfiguration],
             jobReference: util.Optional[JobReference],
             status: util.Optional[JobStatus]) =
    Job(configuration.asScala, jobReference.asScala, status.asScala)

  implicit val format: RootJsonFormat[Job] = jsonFormat3(apply)
}

/**
 * JobConfiguration model
 * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#jobconfiguration BigQuery reference]]
 *
 * @param load configures a load job
 * @param labels the labels associated with this job
 */
final case class JobConfiguration private (load: Option[JobConfigurationLoad], labels: Option[Map[String, String]]) {
  def getLoad = load.asJava
  def getLabels = labels.asJava

  def withLoad(load: Option[JobConfigurationLoad]) =
    copy(load = load)
  def withLoad(load: util.Optional[JobConfigurationLoad]) =
    copy(load = load.asScala)

  def withLabels(labels: Option[Map[String, String]]) =
    copy(labels = labels)
  def withLabels(labels: util.Optional[util.Map[String, String]]) =
    copy(labels = labels.asScala.map(_.asScala.toMap))
}

object JobConfiguration {

  /**
   * JobConfiguration model
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#jobconfiguration BigQuery reference]]
   *
   * @param load configures a load job
   * @return a [[JobConfiguration]]
   */
  def apply(load: Option[JobConfigurationLoad]): JobConfiguration =
    apply(load, None)

  /**
   * Java API: JobConfiguration model
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#jobconfiguration BigQuery reference]]
   *
   * @param load configures a load job
   * @return a [[JobConfiguration]]
   */
  def create(load: util.Optional[JobConfigurationLoad]) =
    JobConfiguration(load.asScala)

  /**
   * Java API: JobConfiguration model
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#jobconfiguration BigQuery reference]]
   *
   * @param load configures a load job
   * @param labels the labels associated with this job
   * @return a [[JobConfiguration]]
   */
  def create(load: util.Optional[JobConfigurationLoad], labels: util.Optional[util.Map[String, String]]) =
    JobConfiguration(load.asScala, labels.asScala.map(_.asScala.toMap))

  implicit val format: JsonFormat[JobConfiguration] = jsonFormat2(apply)
}

/**
 * JobConfigurationLoad model
 * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#jobconfigurationload BigQuery reference]]
 *
 * @param schema the schema for the destination table
 * @param destinationTable the destination table to load the data into
 * @param createDisposition specifies whether the job is allowed to create new tables
 * @param writeDisposition specifies the action that occurs if the destination table already exists
 * @param sourceFormat the format of the data files
 */
final case class JobConfigurationLoad(schema: Option[TableSchema],
                                      destinationTable: Option[TableReference],
                                      createDisposition: Option[CreateDisposition],
                                      writeDisposition: Option[WriteDisposition],
                                      sourceFormat: Option[SourceFormat]) {

  def getSchema = schema.asJava
  def getDestinationTable = destinationTable.asJava
  def getCreateDisposition = createDisposition.asJava
  def getWriteDisposition = writeDisposition.asJava
  def getSourceFormat = sourceFormat.asJava

  def withSchema(schema: Option[TableSchema]) =
    copy(schema = schema)
  def withSchema(schema: util.Optional[TableSchema]) =
    copy(schema = schema.asScala)

  def withDestinationTable(destinationTable: Option[TableReference]) =
    copy(destinationTable = destinationTable)
  def withDestinationTable(destinationTable: util.Optional[TableReference]) =
    copy(destinationTable = destinationTable.asScala)

  def withCreateDisposition(createDisposition: Option[CreateDisposition]) =
    copy(createDisposition = createDisposition)
  def withCreateDisposition(createDisposition: util.Optional[CreateDisposition]) =
    copy(createDisposition = createDisposition.asScala)

  def withWriteDisposition(writeDisposition: Option[WriteDisposition]) =
    copy(writeDisposition = writeDisposition)
  def withWriteDisposition(writeDisposition: util.Optional[WriteDisposition]) =
    copy(writeDisposition = writeDisposition.asScala)

  def withSourceFormat(sourceFormat: Option[SourceFormat]) =
    copy(sourceFormat = sourceFormat)
  def withSourceFormat(sourceFormat: util.Optional[SourceFormat]) =
    copy(sourceFormat = sourceFormat.asScala)
}

object JobConfigurationLoad {

  /**
   * Java API: JobConfigurationLoad model
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#jobconfigurationload BigQuery reference]]
   *
   * @param schema the schema for the destination table
   * @param destinationTable the destination table to load the data into
   * @param createDisposition specifies whether the job is allowed to create new tables
   * @param writeDisposition specifies the action that occurs if the destination table already exists
   * @param sourceFormat the format of the data files
   * @return a [[JobConfigurationLoad]]
   */
  def create(schema: util.Optional[TableSchema],
             destinationTable: util.Optional[TableReference],
             createDisposition: util.Optional[CreateDisposition],
             writeDisposition: util.Optional[WriteDisposition],
             sourceFormat: util.Optional[SourceFormat]) =
    JobConfigurationLoad(
      schema.asScala,
      destinationTable.asScala,
      createDisposition.asScala,
      writeDisposition.asScala,
      sourceFormat.asScala
    )

  implicit val configurationLoadFormat: JsonFormat[JobConfigurationLoad] = jsonFormat5(apply)
}

final case class CreateDisposition(value: String) extends StringEnum
object CreateDisposition {

  /**
   * Java API
   */
  def create(value: String) = CreateDisposition(value)

  val CreateIfNeeded = CreateDisposition("CREATE_IF_NEEDED")
  def createIfNeeded = CreateIfNeeded

  val CreateNever = CreateDisposition("CREATE_NEVER")
  def createNever = CreateNever

  implicit val format: JsonFormat[CreateDisposition] = StringEnum.jsonFormat(apply)
}

final case class WriteDisposition(value: String) extends StringEnum
object WriteDisposition {

  /**
   * Java API
   */
  def create(value: String) = WriteDisposition(value)

  val WriteTruncate = WriteDisposition("WRITE_TRUNCATE")
  def writeTruncate = WriteTruncate

  val WriteAppend = WriteDisposition("WRITE_APPEND")
  def writeAppend = WriteAppend

  val WriteEmpty = WriteDisposition("WRITE_EMPTY")
  def writeEmpty = WriteEmpty

  implicit val format: JsonFormat[WriteDisposition] = StringEnum.jsonFormat(apply)
}

sealed case class SourceFormat(value: String) extends StringEnum
object SourceFormat {

  /**
   * Java API
   */
  def create(value: String) = SourceFormat(value)

  val NewlineDelimitedJsonFormat = SourceFormat("NEWLINE_DELIMITED_JSON")
  def newlineDelimitedJsonFormat = NewlineDelimitedJsonFormat

  implicit val format: JsonFormat[SourceFormat] = StringEnum.jsonFormat(apply)
}

/**
 * JobReference model
 * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/JobReference BigQuery reference]]
 *
 * @param projectId the ID of the project containing this job.
 * @param jobId the ID of the job
 * @param location the geographic location of the job
 */
final case class JobReference(projectId: Option[String], jobId: Option[String], location: Option[String]) {

  @silent("never used")
  @JsonCreator
  private def this(@JsonProperty("projectId") projectId: String,
                   @JsonProperty("jobId") jobId: String,
                   @JsonProperty("location") location: String) =
    this(Option(projectId), Option(jobId), Option(location))

  def getProjectId = projectId.asJava
  def getJobId = jobId.asJava
  def getLocation = location.asJava

  def withProjectId(projectId: Option[String]) =
    copy(projectId = projectId)
  def withProjectId(projectId: util.Optional[String]) =
    copy(projectId = projectId.asScala)

  def withJobId(jobId: Option[String]) =
    copy(jobId = jobId)
  def withJobId(jobId: util.Optional[String]) =
    copy(jobId = jobId.asScala)

  def withLocation(location: Option[String]) =
    copy(location = location)
  def withLocation(location: util.Optional[String]) =
    copy(location = location.asScala)
}

object JobReference {

  /**
   * Java API: JobReference model
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/JobReference BigQuery reference]]
   *
   * @param projectId the ID of the project containing this job.
   * @param jobId the ID of the job
   * @param location the geographic location of the job
   * @return a [[JobReference]]
   */
  def create(projectId: util.Optional[String], jobId: util.Optional[String], location: util.Optional[String]) =
    JobReference(projectId.asScala, jobId.asScala, location.asScala)

  implicit val format: JsonFormat[JobReference] = jsonFormat3(apply)
}

/**
 * JobStatus model
 * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#jobstatus BigQuery reference]]
 *
 * @param errorResult final error result of the job; if present, indicates that the job has completed and was unsuccessful
 * @param errors the first errors encountered during the running of the job
 * @param state running state of the job
 */
final case class JobStatus(errorResult: Option[ErrorProto], errors: Option[Seq[ErrorProto]], state: JobState) {

  def getErrorResult = errorResult.asJava
  def getErrors = errors.map(_.asJava).asJava
  def getState = state

  def withErrorResult(errorResult: Option[ErrorProto]) =
    copy(errorResult = errorResult)
  def withErrorResult(errorResult: util.Optional[ErrorProto]) =
    copy(errorResult = errorResult.asScala)

  def withErrors(errors: Option[Seq[ErrorProto]]) =
    copy(errors = errors)
  def withErrors(errors: util.Optional[util.List[ErrorProto]]) =
    copy(errors = errors.asScala.map(_.asScala.toList))

  def withState(state: JobState) =
    copy(state = state)
}

object JobStatus {

  /**
   * Java API: JobStatus model
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#jobstatus BigQuery reference]]
   *
   * @param errorResult final error result of the job; if present, indicates that the job has completed and was unsuccessful
   * @param errors the first errors encountered during the running of the job
   * @param state running state of the job
   * @return a [[JobStatus]]
   */
  def create(errorResult: util.Optional[ErrorProto], errors: util.Optional[util.List[ErrorProto]], state: JobState) =
    JobStatus(errorResult.asScala, errors.asScala.map(_.asScala.toList), state)

  implicit val format: JsonFormat[JobStatus] = jsonFormat3(apply)
}

final case class JobState(value: String) extends StringEnum
object JobState {

  /**
   * Java API
   */
  def create(value: String) = JobState(value)

  val Pending = JobState("PENDING")
  def pending = Pending

  val Running = JobState("RUNNING")
  def running = Running

  val Done = JobState("DONE")
  def done = Done

  implicit val format: JsonFormat[JobState] = StringEnum.jsonFormat(apply)
}

final case class JobCancelResponse(job: Job) {
  def getJob = job
  def withJob(job: Job) =
    copy(job = job)
}

object JobCancelResponse {

  /**
   * Java API
   */
  def create(job: Job) = JobCancelResponse(job)

  implicit val format: RootJsonFormat[JobCancelResponse] = jsonFormat1(apply)
}
