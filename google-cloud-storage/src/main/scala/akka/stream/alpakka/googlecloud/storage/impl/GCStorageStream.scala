/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage.impl

import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.dispatch.ExecutionContexts.parasitic
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.HttpMethods.{DELETE, POST}
import akka.http.scaladsl.model.Uri.{Path, Query}
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, FromResponseUnmarshaller, Unmarshal, Unmarshaller}
import akka.stream.alpakka.google._
import akka.stream.alpakka.google.auth.{Credentials, ServiceAccountCredentials}
import akka.stream.alpakka.google.http.GoogleHttp
import akka.stream.alpakka.google.implicits._
import akka.stream.alpakka.google.scaladsl.{`X-Upload-Content-Type`, Paginated}
import akka.stream.alpakka.googlecloud.storage._
import akka.stream.alpakka.googlecloud.storage.impl.Formats._
import akka.stream.scaladsl.{Keep, RunnableGraph, Sink, Source}
import akka.stream.{Attributes, Materializer}
import akka.util.ByteString
import akka.{Done, NotUsed}
import com.github.ghik.silencer.silent
import spray.json._

import scala.concurrent.Future

@InternalApi private[storage] object GCStorageStream {
  private val baseUrl = "https://storage.googleapis.com/"
  private val basePath = "/storage/v1"

  def getBucketSource(bucketName: String): Source[Option[Bucket], NotUsed] = {
    val uri = Uri(baseUrl).withPath(Path(basePath) ++ getBucketPath(bucketName))
    val request = HttpRequest(uri = uri)
    makeRequestSource[Option[Bucket]](request)
  }

  def getBucket(bucketName: String)(implicit mat: Materializer, attr: Attributes): Future[Option[Bucket]] =
    getBucketSource(bucketName).withAttributes(attr).runWith(Sink.head)

  def createBucketSource(bucketName: String, location: String): Source[Bucket, NotUsed] = source { settings =>
    val uri = Uri(baseUrl).withPath(Path(basePath) / "b").withQuery(Query("project" -> settings.projectId))
    implicit val ec = parasitic
    val request = Marshal(BucketInfo(bucketName, location)).to[RequestEntity].map { entity =>
      HttpRequest(POST, uri, entity = entity)
    }
    makeRequestSource[Bucket](request)
  }

  def createBucket(bucketName: String, location: String)(implicit mat: Materializer, attr: Attributes): Future[Bucket] =
    createBucketSource(bucketName, location).withAttributes(attr).runWith(Sink.head)

  def deleteBucketSource(bucketName: String): Source[Done, NotUsed] = source { settings =>
    val uri = Uri(baseUrl).withPath(Path(basePath) ++ getBucketPath(bucketName))
    val request = HttpRequest(DELETE, uri)
    makeRequestSource[Done](request)
  }

  def deleteBucket(bucketName: String)(implicit mat: Materializer, attr: Attributes): Future[Done] =
    deleteBucketSource(bucketName).withAttributes(attr).runWith(Sink.head)

  def listBucket(bucket: String, prefix: Option[String], versions: Boolean = false): Source[StorageObject, NotUsed] = {
    val query = ("versions" -> versions.toString) +: ("prefix" -> prefix) ?+: Query.Empty
    val uri = Uri(baseUrl).withPath(Path(basePath) ++ getBucketPath(bucket) / "o").withQuery(query)
    val request = HttpRequest(uri = uri)
    implicit val paginated: Paginated[Option[BucketListResult]] = _.flatMap(_.nextPageToken)
    PaginatedRequest[Option[BucketListResult]](request).mapConcat(_.fold(List.empty[StorageObject])(_.items))
  }

  def getObject(bucket: String,
                objectName: String,
                generation: Option[Long] = None): Source[Option[StorageObject], NotUsed] = {
    val uri = Uri(baseUrl)
      .withPath(Path(basePath) ++ getObjectPath(bucket, objectName))
      .withQuery(Query(generation.map("generation" -> _.toString).toMap))
    val request = HttpRequest(uri = uri)
    makeRequestSource[Option[StorageObject]](request)
  }

  def deleteObjectSource(bucket: String,
                         objectName: String,
                         generation: Option[Long] = None): Source[Boolean, NotUsed] = {
    val uri = Uri(baseUrl)
      .withPath(Path(basePath) ++ getObjectPath(bucket, objectName))
      .withQuery(Query(generation.map("generation" -> _.toString).toMap))
    val request = HttpRequest(DELETE, uri)
    makeRequestSource[Option[Done]](request).map(_.isDefined)
  }

  def deleteObjectsByPrefixSource(bucket: String, prefix: Option[String]): Source[Boolean, NotUsed] =
    listBucket(bucket, prefix)
      .flatMapConcat(listBucketResult => deleteObjectSource(bucket, listBucketResult.name))

  def putObject(bucket: String,
                objectName: String,
                data: Source[ByteString, _],
                contentType: ContentType): Source[StorageObject, NotUsed] = {
    val uri = Uri(baseUrl)
      .withPath(Path("/upload" + basePath) ++ getBucketPath(bucket) / "o")
      .withQuery(Query("uploadType" -> "media", "name" -> objectName))
    val entity = HttpEntity(contentType, data)
    val request = HttpRequest(POST, uri, entity = entity)
    makeRequestSource[StorageObject](request)
  }

  def download(bucket: String,
               objectName: String,
               generation: Option[Long] = None): Source[Option[Source[ByteString, NotUsed]], NotUsed] = {
    val query = ("alt" -> "media") +: ("generation" -> generation.map(_.toString)) ?+: Query.Empty
    val uri = Uri(baseUrl)
      .withPath(Path(basePath) ++ getObjectPath(bucket, objectName))
      .withQuery(query)
    val request = HttpRequest(uri = uri)
    implicit val um: Unmarshaller[HttpEntity, Source[ByteString, NotUsed]] =
      Unmarshaller.strict(_.withoutSizeLimit.dataBytes.mapMaterializedValue(_ => NotUsed))
    makeRequestSource[Option[Source[ByteString, NotUsed]]](request)
  }

  def resumableUpload(bucket: String,
                      objectName: String,
                      contentType: ContentType,
                      chunkSize: Int = 5 * 1024 * 1024,
                      metadata: Option[Map[String, String]] = None): Sink[ByteString, Future[StorageObject]] =
    Sink
      .fromMaterializer { (mat, attr) =>
        implicit val settings = {
          val s = resolveSettings(mat, attr)
          s.copy(requestSettings = s.requestSettings.copy(uploadChunkSize = chunkSize))
        }

        val uri = Uri(baseUrl)
          .withPath(Path("/upload" + basePath) ++ getBucketPath(bucket) / "o")
          .withQuery(("uploadType" -> "resumable") +: ("name" -> objectName) +: Query.Empty)
        val headers = List(`X-Upload-Content-Type`(contentType))
        val entity =
          metadata.fold(HttpEntity.Empty)(m => HttpEntity(ContentTypes.`application/json`, m.toJson.toString))
        val request = HttpRequest(POST, uri, headers, entity)

        implicit val um: Unmarshaller[HttpResponse, StorageObject] = Unmarshaller.withMaterializer {
          implicit ec => implicit mat =>
            {
              case HttpResponse(status, _, entity, _) if status.isSuccess() =>
                Unmarshal(entity).to[StorageObject]
              case HttpResponse(status, _, entity, _) =>
                Unmarshal(entity).to[String].flatMap { errorString =>
                  Future.failed(new RuntimeException(s"Uploading part failed with status $status: $errorString"))
                }
            }: PartialFunction[HttpResponse, Future[StorageObject]]
        }.withDefaultRetry

        ResumableUpload[StorageObject](request).addAttributes(GoogleAttributes.settings(settings))
      }
      .mapMaterializedValue(_.flatten)

  def rewrite(sourceBucket: String,
              sourceObjectName: String,
              destinationBucket: String,
              destinationObjectName: String): RunnableGraph[Future[StorageObject]] = {

    sealed trait RewriteState
    case object Starting extends RewriteState
    case class Running(rewriteToken: String) extends RewriteState
    case object Finished extends RewriteState

    val sourcePath = getObjectPath(sourceBucket, sourceObjectName)
    val destinationPath = getObjectPath(destinationBucket, destinationObjectName)

    def rewriteRequest(rewriteToken: Option[String]): Source[Option[(RewriteState, RewriteResponse)], NotUsed] = {
      val query = ("rewriteToken" -> rewriteToken) ?+: Query.Empty
      val uri = Uri(baseUrl)
        .withPath(Path(basePath) ++ sourcePath / "rewriteTo" ++ destinationPath)
        .withQuery(query)
      val entity = HttpEntity.Empty.withoutSizeLimit()
      val request = HttpRequest(POST, uri, entity = entity)
      makeRequestSource[RewriteResponse](request).map { rewriteResponse =>
        Some(
          rewriteResponse.rewriteToken.fold[(RewriteState, RewriteResponse)]((Finished, rewriteResponse))(
            token => (Running(token), rewriteResponse)
          )
        )
      }
    }

    Source
      .fromMaterializer { (mat, attr) =>
        Source
          .unfoldAsync[RewriteState, RewriteResponse](Starting) {
            case Finished => Future.successful(None)
            case Starting => rewriteRequest(None).runWith(Sink.head)(mat)
            case Running(token) => rewriteRequest(Some(token)).runWith(Sink.head)(mat)
          }
      }
      .toMat(Sink.last[RewriteResponse])(Keep.right)
      .mapMaterializedValue(
        _.flatMap(
          _.resource match {
            case Some(resource) => Future.successful(resource)
            case None => Future.failed(new RuntimeException("Storage object is missing"))
          }
        )(ExecutionContexts.parasitic)
      )
  }

  private def makeRequestSource[T: FromResponseUnmarshaller](request: HttpRequest): Source[T, NotUsed] =
    makeRequestSource[T](Future.successful(request))

  private def makeRequestSource[T: FromResponseUnmarshaller](request: Future[HttpRequest]): Source[T, NotUsed] =
    Source
      .fromMaterializer { (mat, attr) =>
        implicit val settings = resolveSettings(mat, attr)
        Source.lazyFuture { () =>
          request.flatMap { request =>
            GoogleHttp()(mat.system).singleAuthenticatedRequest[T](request)
          }(ExecutionContexts.parasitic)
        }
      }
      .mapMaterializedValue(_ => NotUsed)

  private def getBucketPath(bucket: String) =
    Path("/b") / bucket

  private def getObjectPath(bucket: String, objectName: String) =
    getBucketPath(bucket) / "o" / objectName

  implicit def unmarshaller[T: FromEntityUnmarshaller]: Unmarshaller[HttpResponse, T] =
    Unmarshaller.withMaterializer { implicit ec => implicit mat => response: HttpResponse =>
      response match {
        case HttpResponse(status, _, entity, _) if status.isSuccess() && !status.isRedirection() =>
          Unmarshal(entity).to[T]
        case HttpResponse(status, _, entity, _) =>
          Unmarshal(entity).to[String].flatMap { err =>
            Future.failed(new RuntimeException(s"[${status.intValue}] $err"))
          }
      }
    }.withDefaultRetry

  implicit def optionUnmarshaller[T: FromEntityUnmarshaller]: Unmarshaller[HttpResponse, Option[T]] =
    Unmarshaller.withMaterializer { implicit ec => implicit mat => response: HttpResponse =>
      response match {
        case HttpResponse(status, _, entity, _) if status.isSuccess() && !status.isRedirection() =>
          Unmarshal(entity).to[T].map(Some(_))
        case HttpResponse(StatusCodes.NotFound, _, entity, _) =>
          entity.discardBytes()
          Future.successful(None)
        case HttpResponse(status, _, entity, _) =>
          Unmarshal(entity).to[String].flatMap { err =>
            Future.failed(new RuntimeException(s"[${status.intValue}] $err"))
          }
      }
    }.withDefaultRetry

  implicit private val doneUnmarshaller: Unmarshaller[HttpEntity, Done] =
    Unmarshaller.withMaterializer { _ => implicit mat => entity =>
      entity.discardBytes().future
    }

  private def source[T](f: GoogleSettings => Source[T, NotUsed]): Source[T, NotUsed] =
    Source
      .fromMaterializer { (mat, attr) =>
        f(resolveSettings(mat, attr))
      }
      .mapMaterializedValue(_ => NotUsed)

  @silent("deprecated")
  private def resolveSettings(mat: Materializer, attr: Attributes) = {
    implicit val sys = mat.system
    val legacySettings = attr
      .get[GCStorageSettingsValue]
      .map(_.settings)
      .getOrElse {
        val configPath = attr.get[GCStorageSettingsPath](GCStorageSettingsPath.Default).path
        GCStorageExt(sys).settings(configPath)
      }

    val settings = GoogleAttributes.resolveSettings(mat, attr)

    if (legacySettings.privateKey == "deprecated")
      GoogleAttributes.resolveSettings(mat, attr)
    else {
      sys.log.warning("Configuration via alpakka.google.cloud.storage is deprecated")

      require(
        (legacySettings.baseUrl.contains("googleapis.com") || legacySettings.baseUrl == "unsupported")
        && (legacySettings.basePath.contains("storage/v1") || legacySettings.basePath == "unsupported")
        && (legacySettings.tokenUrl.contains("googleapis.com") || legacySettings.tokenUrl == "unsupported"),
        "Non-default base-url/base-path/token-url no longer supported, use config path alpakka.google.forward-proxy"
      )

      val legacyScopes = legacySettings.tokenScope.split(" ").toList
      val credentials = Credentials.cache(
        (
          legacySettings.projectId,
          legacySettings.clientEmail,
          legacySettings.privateKey,
          legacyScopes,
          mat.system.name
        )
      ) {
        ServiceAccountCredentials(
          legacySettings.projectId,
          legacySettings.clientEmail,
          legacySettings.privateKey,
          legacyScopes
        )
      }

      GoogleSettings(
        legacySettings.projectId,
        credentials,
        settings.requestSettings
      )
    }
  }
}
