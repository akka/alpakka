/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.model

import akka.stream.alpakka.googlecloud.bigquery.model.ErrorProtoJsonProtocol.ErrorProto
import akka.stream.alpakka.googlecloud.bigquery.model.TableJsonProtocol.{TableReference, TableSchema}
import spray.json.{DefaultJsonProtocol, JsonFormat, RootJsonFormat}

object JobJsonProtocol extends DefaultJsonProtocol {

  final case class Job(configuration: Option[JobConfiguration],
                       jobReference: Option[JobReference],
                       status: Option[JobStatus])
  final case class JobConfiguration(load: Option[JobConfigurationLoad])
  final case class JobConfigurationLoad(schema: Option[TableSchema],
                                        destinationTable: Option[TableReference],
                                        createDisposition: Option[String],
                                        writeDisposition: Option[String],
                                        sourceFormat: Option[String])
  val CreateIfNeededDisposition = "CREATE_IF_NEEDED"
  val CreateNeverDisposition = "CREATE_NEVER"
  val WriteTruncateDisposition = "WRITE_TRUNCATE"
  val WriteAppendDisposition = "WRITE_APPEND"
  val WriteEmptyDisposition = "WRITE_EMPTY"
  val NewlineDelimitedJsonFormat = "NEWLINE_DELIMITED_JSON"
  final case class JobReference(projectId: Option[String], jobId: Option[String], location: Option[String])
  final case class JobStatus(errorResult: Option[ErrorProto], errors: Option[Seq[ErrorProto]], state: String)
  val PendingState = "PENDING"
  val RunningState = "RUNNING"
  val DoneState = "DONE"

  implicit val configurationLoadFormat: JsonFormat[JobConfigurationLoad] = jsonFormat5(JobConfigurationLoad)
  implicit val configurationFormat: JsonFormat[JobConfiguration] = jsonFormat1(JobConfiguration)
  implicit val referenceFormat: JsonFormat[JobReference] = jsonFormat3(JobReference)
  implicit val statusFormat: JsonFormat[JobStatus] = jsonFormat3(JobStatus)
  implicit val format: RootJsonFormat[Job] = jsonFormat3(Job)
}
