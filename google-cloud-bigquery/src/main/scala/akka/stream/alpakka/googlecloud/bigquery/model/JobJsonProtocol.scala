/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.model

import akka.stream.alpakka.googlecloud.bigquery.model.ErrorProtoJsonProtocol.ErrorProto
import akka.stream.alpakka.googlecloud.bigquery.model.TableJsonProtocol.{TableReference, TableSchema}
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray.BigQueryRestJsonProtocol._
import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import com.github.ghik.silencer.silent
import spray.json.{deserializationError, JsString, JsValue, JsonFormat, RootJsonFormat}

import java.util
import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.compat.java8.OptionConverters._

object JobJsonProtocol {

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

    def withConfiguration(configuration: util.Optional[JobConfiguration]) =
      copy(configuration = configuration.asScala)
    def withJobReference(jobReference: util.Optional[JobReference]) =
      copy(jobReference = jobReference.asScala)
    def withStatus(status: util.Optional[JobStatus]) =
      copy(status = status.asScala)
  }

  /**
   * Java API: Job model
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/Job BigQuery reference]]
   *
   * @param configuration describes the job configuration
   * @param jobReference reference describing the unique-per-user name of the job
   * @param status the status of this job
   * @return a [[Job]]
   */
  def createJob(configuration: util.Optional[JobConfiguration],
                jobReference: util.Optional[JobReference],
                status: util.Optional[JobStatus]) =
    Job(configuration.asScala, jobReference.asScala, status.asScala)

  /**
   * JobConfiguration model
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#jobconfiguration BigQuery reference]]
   *
   * @param load configures a load job
   */
  final case class JobConfiguration(load: Option[JobConfigurationLoad]) {
    def getLoad = load.asJava
    def withLoad(load: util.Optional[JobConfigurationLoad]) = copy(load = load.asScala)
  }

  /**
   * Java API: JobConfiguration model
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#jobconfiguration BigQuery reference]]
   *
   * @param load configures a load job
   * @return a [[JobConfiguration]]
   */
  def createJobConfiguration(load: util.Optional[JobConfigurationLoad]) =
    JobConfiguration(load.asScala)

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

    def withSchema(schema: util.Optional[TableSchema]) =
      copy(schema = schema.asScala)
    def withDestinationTable(destinationTable: util.Optional[TableReference]) =
      copy(destinationTable = destinationTable.asScala)
    def withCreateDisposition(createDisposition: util.Optional[CreateDisposition]) =
      copy(createDisposition = createDisposition.asScala)
    def withWriteDisposition(writeDisposition: util.Optional[WriteDisposition]) =
      copy(writeDisposition = writeDisposition.asScala)
    def withSourceFormat(sourceFormat: util.Optional[SourceFormat]) =
      copy(sourceFormat = sourceFormat.asScala)
  }

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
  def createJobConfigurationLoad(schema: util.Optional[TableSchema],
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

  final case class CreateDisposition(value: String) {
    def getValue = value
  }
  val CreateIfNeededDisposition = CreateDisposition("CREATE_IF_NEEDED")
  val CreateNeverDisposition = CreateDisposition("CREATE_NEVER")

  def createCreateDisposition(value: String) = CreateDisposition(value)

  final case class WriteDisposition(value: String) {
    def getValue = value
  }
  val WriteTruncateDisposition = WriteDisposition("WRITE_TRUNCATE")
  val WriteAppendDisposition = WriteDisposition("WRITE_APPEND")
  val WriteEmptyDisposition = WriteDisposition("WRITE_EMPTY")

  /**
   * Java API
   */
  def createWriteDisposition(value: String) = WriteDisposition(value)

  sealed case class SourceFormat(value: String) {
    def getValue = value
  }
  val NewlineDelimitedJsonFormat = SourceFormat("NEWLINE_DELIMITED_JSON")

  /**
   * Java API
   */
  def createSourceFormat(value: String) = SourceFormat(value)

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

    def withProjectId(projectId: util.Optional[String]) =
      copy(projectId = projectId.asScala)
    def withJobId(jobId: util.Optional[String]) =
      copy(jobId = jobId.asScala)
    def withLocation(location: util.Optional[String]) =
      copy(location = location.asScala)
  }

  /**
   * Java API: JobReference model
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/JobReference BigQuery reference]]
   *
   * @param projectId the ID of the project containing this job.
   * @param jobId the ID of the job
   * @param location the geographic location of the job
   * @return a [[JobReference]]
   */
  def createJobReference(projectId: util.Optional[String],
                         jobId: util.Optional[String],
                         location: util.Optional[String]) =
    JobReference(projectId.asScala, jobId.asScala, location.asScala)

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

    def withErrorResult(errorResult: util.Optional[ErrorProto]) =
      copy(errorResult = errorResult.asScala)
    def withErrors(errors: util.Optional[util.List[ErrorProto]]) =
      copy(errors = errors.asScala.map(_.asScala.toList))
    def withState(state: JobState) =
      copy(state = state)
  }

  /**
   * Java API: JobStatus model
   * @see [[https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#jobstatus BigQuery reference]]
   *
   * @param errorResult final error result of the job; if present, indicates that the job has completed and was unsuccessful
   * @param errors the first errors encountered during the running of the job
   * @param state running state of the job
   * @return a [[JobStatus]]
   */
  def createJobStatus(errorResult: util.Optional[ErrorProto],
                      errors: util.Optional[util.List[ErrorProto]],
                      state: JobState) =
    JobStatus(errorResult.asScala, errors.asScala.map(_.asScala.toList), state)

  final case class JobState(value: String) {
    def getValue = value
  }
  val PendingState = JobState("PENDING")
  val RunningState = JobState("RUNNING")
  val DoneState = JobState("DONE")

  /**
   * Java API
   */
  def createJobState(value: String) = JobState(value)

  /** Java API */
  def pendingState = PendingState

  /** Java API */
  def runningState = RunningState

  /** Java API */
  def doneState = DoneState

  final case class JobCancelResponse(job: Job) {
    def getJob = job
    def withJob(job: Job) =
      copy(job = job)
  }

  /**
   * Java API
   */
  def createJobCancelResponse(job: Job) =
    JobCancelResponse(job)

  implicit val createDispositionFormat: JsonFormat[CreateDisposition] = new JsonFormat[CreateDisposition] {
    override def read(json: JsValue): CreateDisposition = json match {
      case JsString(x) => CreateDisposition(x)
      case x => deserializationError("Expected CreateDisposition as JsString, but got " + x)
    }
    override def write(obj: CreateDisposition): JsValue = JsString(obj.value)
  }
  implicit val writeDispositionFormat: JsonFormat[WriteDisposition] = new JsonFormat[WriteDisposition] {
    override def read(json: JsValue): WriteDisposition = json match {
      case JsString(x) => WriteDisposition(x)
      case x => deserializationError("Expected WriteDisposition as JsString, but got " + x)
    }
    override def write(obj: WriteDisposition): JsValue = JsString(obj.value)
  }
  implicit val sourceFormatFormat: JsonFormat[SourceFormat] = new JsonFormat[SourceFormat] {
    override def read(json: JsValue): SourceFormat = json match {
      case JsString(x) => SourceFormat(x)
      case x => deserializationError("Expected SourceFormat as JsString, but got " + x)
    }
    override def write(obj: SourceFormat): JsValue = JsString(obj.value)
  }
  implicit val configurationLoadFormat: JsonFormat[JobConfigurationLoad] = jsonFormat5(JobConfigurationLoad)
  implicit val configurationFormat: JsonFormat[JobConfiguration] = jsonFormat1(JobConfiguration)
  implicit val referenceFormat: JsonFormat[JobReference] = jsonFormat3(JobReference)
  implicit val jobStateFormat: JsonFormat[JobState] = new JsonFormat[JobState] {
    override def read(json: JsValue): JobState = json match {
      case JsString(x) => JobState(x)
      case x => deserializationError("Expected JobState as JsString, but got " + x)
    }
    override def write(obj: JobState): JsValue = JsString(obj.value)
  }
  implicit val statusFormat: JsonFormat[JobStatus] = jsonFormat3(JobStatus)
  implicit val format: RootJsonFormat[Job] = jsonFormat3(Job)
  implicit val cancelResponseFormat: RootJsonFormat[JobCancelResponse] = jsonFormat1(JobCancelResponse)
}
