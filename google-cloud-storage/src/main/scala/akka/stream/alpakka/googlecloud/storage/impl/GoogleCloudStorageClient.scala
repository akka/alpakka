/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage.impl

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.Uri.{Path, Query}
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{`Content-Range`, OAuth2BearerToken, RawHeader}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer
import akka.stream.alpakka.googlecloud.storage.GoogleAuthConfiguration
import akka.stream.alpakka.googlecloud.storage.Model._
import akka.stream.alpakka.googlecloud.storage.impl.Formats._
import akka.stream.alpakka.googlecloud.storage.impl.GoogleCloudStorageClient._
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.util.ByteString
import spray.json._

import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

object GoogleCloudStorageClient {
  private val baseUri = Uri("https://www.googleapis.com/")
  private val basePath = Path("/storage/v1")

  def apply(authConfiguration: GoogleAuthConfiguration)(implicit system: ActorSystem, mat: Materializer) =
    new GoogleCloudStorageClient(authConfiguration)
}

final class GoogleCloudStorageClient(authConfiguration: GoogleAuthConfiguration)(implicit system: ActorSystem,
                                                                                 mat: Materializer) {

  private lazy val gsSesion = Session(authConfiguration, Seq("https://www.googleapis.com/auth/devstorage.read_write"))
  private implicit val ec = mat.executionContext

  def getBucket(bucketName: String): Future[Option[BucketInfo]] =
    request(getBucketPath(bucketName)).flatMap(entityForSuccessOption).flatMap(responseEntityOptionTo[BucketInfo])

  def createBucket(bucketName: String, location: String): Future[BucketInfo] =
    postRequest(
      basePath / "b",
      Map("project" -> authConfiguration.projectId),
      ContentTypes.`application/json`,
      ByteString(BucketInfo(bucketName, location).toJson.compactPrint)
    ).flatMap(entityForSuccess)
      .flatMap(responseEntityTo[BucketInfo])

  def deleteBucket(bucketName: String): Future[Done] =
    request(getBucketPath(bucketName), HttpMethods.DELETE).flatMap { resp =>
      if (resp.status == StatusCodes.NoContent) {
        Future.successful(Done)
      } else {
        Unmarshal(resp.entity).to[String].flatMap { err =>
          Future.failed(new RuntimeException(err))
        }
      }
    }

  def listBucket(bucket: String, prefix: Option[String]): Source[StorageObject, NotUsed] =
    Source
      .fromFuture(
        getBucketListResult(bucket, prefix)
      )
      .mapConcat {
        case None => Seq.empty
        case Some(bucketListResult) => bucketListResult.items
      }
      .filter { so =>
        prefix.forall(!_.equals(so.name))
      }

  def getStorageObject(bucket: String, objectName: String): Future[Option[StorageObject]] =
    request(getObjectPath(bucket, objectName))
      .flatMap(entityForSuccessOption)
      .flatMap(responseEntityOptionTo[StorageObject])

  /**
   * Returns an empty Source if the object was not found
   * Returns a Source with a single empty ByteString if found but empty
   *
   */
  def download(bucket: String, objectName: String): Source[ByteString, Future[Seq[HttpHeader]]] = {
    val future = request(getObjectPath(bucket, objectName), queryParams = Map("alt" -> "media"))
    Source
      .fromFuture(future.flatMap(entityForSuccessOption))
      .flatMapConcat {
        case Some(entity) => entity.dataBytes
        case None => Source.empty
      }
      .mapMaterializedValue { _ =>
        // TODO parse metadata?
        future.map(_.headers)
      }
  }

  def upload(bucket: String, objectName: String, contentType: ContentType, bytes: ByteString): Future[StorageObject] =
    postRequest(Path("/upload") ++ basePath / "b" / bucket / "o",
                Map("uploadType" -> "media", "name" -> objectName),
                contentType,
                bytes)
      .flatMap(entityForSuccess)
      .flatMap(responseEntityTo[StorageObject])

  def createUploadSink(bucket: String,
                       objectName: String,
                       contentType: ContentType,
                       chunkSize: Int = 5 * 1024 * 1024): Sink[ByteString, Future[StorageObject]] =
    chunkAndRequest(bucket, objectName, contentType, chunkSize)
      .toMat(completionSink())(Keep.right)

  def delete(bucket: String, objectName: String): Future[Boolean] =
    request(getObjectPath(bucket, objectName), HttpMethods.DELETE)
      .flatMap(entityForSuccessOption)
      .map(option => option.isDefined)

  def deleteFolder(bucket: String, folderName: String): Future[Unit] = {
    val eventualDone = listBucket(bucket, Some(folderName))
      .mapAsync(10) { so =>
        delete(bucket, so.name)
      }
      .runWith(Sink.ignore)
    eventualDone.flatMap { _ =>
      val eventualUnit = exists(bucket, folderName).flatMap {
        case true => delete(bucket, folderName)
        case false => Future.successful(())
      }
      eventualUnit.map(_ => ())

    }
  }

  def exists(bucket: String, objectName: String): Future[Boolean] =
    request(getObjectPath(bucket, objectName), HttpMethods.HEAD).flatMap {
      case HttpResponse(status, _, entity, _) if status.isSuccess() && !status.isRedirection() =>
        entity.discardBytes().future().map(_ => true)
      case HttpResponse(StatusCodes.NotFound, _, entity, _) =>
        entity.discardBytes().future().map(_ => false)
      case HttpResponse(_, _, entity, _) =>
        Unmarshal(entity).to[String].flatMap { err =>
          Future.failed(new RuntimeException(err))
        }
    }

  private def postRequest(
      fullPath: Path,
      queryParams: Map[String, String],
      contentType: ContentType,
      bytes: ByteString
  ): Future[HttpResponse] =
    createAuthenticatedRequest(baseUri.withPath(fullPath).withQuery(Query(queryParams)), HttpMethods.POST)
      .flatMap(req => Http().singleRequest(req.withEntity(contentType, bytes)))

  private def postRequestWithHeaders(
      fullPath: Path,
      queryParams: Map[String, String],
      headers: Seq[HttpHeader]
  ): Future[HttpResponse] =
    createAuthenticatedRequest(baseUri.withPath(fullPath).withQuery(Query(queryParams)), HttpMethods.POST, headers)
      .flatMap(Http().singleRequest(_))

  private def request(
      path: Path,
      method: HttpMethod = HttpMethods.GET,
      queryParams: Map[String, String] = Map.empty
  ): Future[HttpResponse] =
    createAuthenticatedRequest(baseUri.withPath(basePath ++ path).withQuery(Query(queryParams)), method)
      .flatMap(Http().singleRequest(_))

  private def createAuthenticatedRequest(uri: Uri,
                                         method: HttpMethod,
                                         headers: Seq[HttpHeader] = Seq.empty): Future[HttpRequest] =
    gsSesion.getToken().map { accessToken =>
      HttpRequest(method)
        .withUri(uri)
        .withHeaders(headers: _*)
        .addCredentials(OAuth2BearerToken(accessToken))
    }

  private def getBucketPath(bucket: String) =
    Path("/b") / bucket

  private def getBucketListResult(bucket: String,
                                  prefix: Option[String],
                                  pageToken: Option[String] = None): Future[Option[BucketListResult]] = {
    var queryParams = prefix.map(pref => Map("prefix" -> pref)).getOrElse(Map.empty)
    pageToken.foreach(token => queryParams = queryParams + ("pageToken" -> token))
    request(getBucketPath(bucket) / "o", queryParams = queryParams)
      .flatMap(entityForSuccessOption)
      .flatMap(responseEntityOptionTo[BucketListResult])
      .flatMap { bucketListResultOption =>
        val eventualMaybeResults = bucketListResultOption.map { bucketListResult =>
          bucketListResult.nextPageToken match {
            case None => Future.successful(bucketListResult)
            case Some(token) =>
              getBucketListResult(bucket, prefix, Some(token)).map {
                case Some(res) =>
                  res.merge(bucketListResult)
                case None =>
                  bucketListResult
              }
          }
        }
        swap(eventualMaybeResults)
      }
  }

  private def getObjectPath(bucket: String, objectName: String) =
    getBucketPath(bucket) / "o" / objectName

  private def swap[A](option: Option[Future[A]]): Future[Option[A]] =
    option match {
      case Some(f) => f.map(Some(_))
      case None => Future.successful(None)
    }

  private def responseEntityOptionTo[A](
      responseOption: Option[ResponseEntity]
  )(implicit aRead: JsonReader[A], ec: ExecutionContext): Future[Option[A]] =
    swap(responseOption.map(resp => responseEntityTo(resp)(aRead, ec)))

  private def responseEntityTo[A](response: ResponseEntity)(implicit aRead: JsonReader[A],
                                                            ec: ExecutionContext): Future[A] =
    Unmarshal(response).to[JsValue].map { str =>
      str.convertTo[A]
    }

  private def entityForSuccessOption(resp: HttpResponse): Future[Option[ResponseEntity]] =
    resp match {
      case HttpResponse(status, _, entity, _) if status.isSuccess() && !status.isRedirection() =>
        Future.successful(Some(entity))
      case HttpResponse(StatusCodes.NotFound, _, entity, _) =>
        entity.discardBytes()
        Future.successful(None)
      case HttpResponse(_, _, entity, _) =>
        Unmarshal(entity).to[String].flatMap { err =>
          Future.failed(new RuntimeException(err))
        }
    }

  private def entityForSuccess(resp: HttpResponse): Future[ResponseEntity] =
    resp match {
      case HttpResponse(status, _, entity, _) if status.isSuccess() && !status.isRedirection() =>
        Future.successful(entity)
      case HttpResponse(_, _, entity, _) =>
        Unmarshal(entity).to[String].flatMap { err =>
          Future.failed(new RuntimeException(err))
        }
    }

  private def chunkAndRequest(bucket: String,
                              objectName: String,
                              contentType: ContentType,
                              chunkSize: Int): Flow[ByteString, UploadPartResponse, NotUsed] = {

    // Multipart upload requests are created here.
    //  The initial upload request gets executed within this function as well.
    //  The individual upload part requests are created.
    val requestFlow = createRequests(bucket, objectName, contentType, chunkSize)

    // The individual upload part requests are processed here
    // apparently Google Cloud storage does not support parallel uploading
    requestFlow
      .mapAsync(1) {
        case (req, (upload, index)) =>
          Http()
            .singleRequest(req)
            .map(resp => (Success(resp), (upload, index)))
            .recoverWith {
              case NonFatal(e) => Future.successful((Failure(e), (upload, index)))
            }
      }
      .mapAsync(1) {
        // 308 Resume incomplete means that chunk was successfully transfered but more chunks are expected
        case (Success(HttpResponse(StatusCodes.PermanentRedirect, headers, entity, _)), (upload, index)) =>
          entity.discardBytes()
          Future.successful(SuccessfulUploadPart(upload, index))
        case (Success(HttpResponse(status, headers, entity, _)), (upload, index))
            if status.isSuccess() && !status.isRedirection() =>
          responseEntityTo[StorageObject](entity).map(so => SuccessfulUpload(upload, index, so))
        case (Success(HttpResponse(status, _, entity, _)), (upload, index)) =>
          val errorString = Unmarshal(entity).to[String]
          Future.successful(
            FailedUploadPart(upload,
                             index,
                             new RuntimeException(s"Uploading part failed with status $status: $errorString"))
          )
        case (Failure(e), (upload, index)) =>
          Future.successful(FailedUploadPart(upload, index, e))
      }
  }

  private def createRequests(bucketName: String,
                             objectName: String,
                             contentType: ContentType,
                             chunkSize: Int): Flow[ByteString, (HttpRequest, (MultiPartUpload, Int)), NotUsed] = {

    assert(
      (chunkSize >= (256 * 1024)) && (chunkSize % (256 * 1024) == 0),
      "Chunk size must be a multiple of 256K"
    )

    // First step of the resumable upload process is made.
    //  The response is then used to construct the subsequent individual upload part requests
    val requestInfo: Source[(MultiPartUpload, Int), NotUsed] = initiateUpload(bucketName, objectName, contentType)

    Flow
      .apply[ByteString]
      .via(new Chunker(chunkSize))
      .zipWith(requestInfo) {
        case (chunk, info) => (chunk, info)
      }
      .mapAsync(1) {
        case (chunkedPayload, (uploadInfo, chunkIndex)) =>
          createAuthenticatedRequest(
            baseUri
              .withPath(Path("/upload") ++ basePath / "b" / bucketName / "o")
              .withQuery(
                Query(Map("uploadType" -> "resumable", "name" -> objectName, "upload_id" -> uploadInfo.uploadId))
              ),
            HttpMethods.PUT
          ).map { req =>
              // add payload and Content-Range header
              req
                .withEntity(
                  HttpEntity(ContentTypes.`application/octet-stream`,
                             chunkedPayload.size,
                             Source.single(chunkedPayload.bytes))
                )
                // make sure we do these calculations in the Long range !!!! We talk about potentially huge files
                // Int is limited to 2,1Gb !!
                .addHeader(
                  `Content-Range`(
                    ContentRange((chunkIndex - 1l) * chunkSize,
                                 ((chunkIndex - 1l) * chunkSize) + Math.max(chunkedPayload.size, 1l) - 1,
                                 chunkedPayload.totalSize)
                  )
                )
            }
            .map((_, (uploadInfo, chunkIndex)))
      }
  }

  private def initiateUpload(bucketName: String,
                             objectName: String,
                             contentType: ContentType): Source[(MultiPartUpload, Int), NotUsed] =
    Source
      .single((bucketName, objectName))
      .mapAsync(1) { _ =>
        postRequestWithHeaders(
          Path("/upload") ++ basePath / "b" / bucketName / "o",
          Map("uploadType" -> "resumable", "name" -> objectName),
          Seq(RawHeader("X-Upload-Content-Type", contentType.toString()))
        ).flatMap {
          case HttpResponse(status, headers, entity, _) if status.isSuccess() && !status.isRedirection() =>
            entity.discardBytes()
            headers
              .find(_.is("location"))
              .flatMap(h => Uri(h.value()).query().get("upload_id"))
              .map(MultiPartUpload)
              .map(Future.successful)
              .getOrElse(Future.failed(new RuntimeException("No upload_id found in Location Header")))
          case HttpResponse(StatusCodes.NotFound, _, entity, _) =>
            Unmarshal(entity).to[String].flatMap { err =>
              Future.failed(new ObjectNotFoundException(err))
            }
          case HttpResponse(_, _, entity, _) =>
            Unmarshal(entity).to[String].flatMap { err =>
              Future.failed(new RuntimeException(err))
            }
        }
      }
      .mapConcat(r => Stream.continually(r))
      .zip(Source.fromIterator(() => Iterator.from(1)))

  private def completionSink(): Sink[UploadPartResponse, Future[StorageObject]] =
    Sink.seq[UploadPartResponse].mapMaterializedValue { responseFuture: Future[Seq[UploadPartResponse]] =>
      responseFuture
        .flatMap { responses: Seq[UploadPartResponse] =>
          //val successes = responses.collect { case r: SuccessfulUploadPart => r }
          val storageObjectResult = responses.collect { case so: SuccessfulUpload => so }
          val failures = responses.collect { case r: FailedUploadPart => r }
          if (responses.isEmpty) {
            Future.failed(new RuntimeException("No Responses"))
          } else if (failures.isEmpty && storageObjectResult.nonEmpty) {
            Future.successful(storageObjectResult.head.storageObject)
          } else {
            Future.failed(FailedUpload(failures.map(_.exception)))
          }
        }
    }

}
