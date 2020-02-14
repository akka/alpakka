/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage.impl

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.Uri.{Path, Query}
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.alpakka.googlecloud.pubsub.impl.GoogleRetry
import akka.stream.{ActorMaterializer, Attributes, Materializer}
import akka.stream.alpakka.googlecloud.storage._
import akka.stream.alpakka.googlecloud.storage.impl.Formats._
import akka.stream.scaladsl.{Flow, Keep, RunnableGraph, Sink, Source}
import akka.util.ByteString
import spray.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import scala.util.control.NonFatal

@InternalApi private[storage] object GCStorageStream {
  private val parallelism = 1

  def getBucketSource(bucketName: String): Source[Option[Bucket], NotUsed] =
    Source
      .setup { (mat, attr) =>
        implicit val materializer = mat
        implicit val attribures = attr
        makeRequestSource(
          createRequestSource(
            uriFactory = (settings: GCStorageSettings) =>
              Uri(settings.baseUrl).withPath(Path(settings.basePath) ++ getBucketPath(bucketName))
          )
        ).mapAsync(parallelism)(response => processGetBucketResponse(response, mat))
      }
      .mapMaterializedValue(_ => NotUsed)

  def getBucket(bucketName: String)(implicit mat: Materializer, attr: Attributes): Future[Option[Bucket]] =
    getBucketSource(bucketName).withAttributes(attr).runWith(Sink.head)

  def createBucketSource(bucketName: String, location: String): Source[Bucket, NotUsed] =
    Source
      .setup { (mat, attr) =>
        implicit val materializer = mat
        implicit val attribures = attr
        makeRequestSource(
          createPostRequestSource(
            ContentTypes.`application/json`,
            ByteString(BucketInfo(bucketName, location).toJson.compactPrint),
            (settings: GCStorageSettings) =>
              Uri(settings.baseUrl)
                .withPath(Path(settings.basePath) / "b")
                .withQuery(Query(Map("project" -> settings.projectId)))
          )
        ).mapAsync(parallelism)(response => processCreateBucketResponse(response, mat))
      }
      .mapMaterializedValue(_ => NotUsed)

  def createBucket(bucketName: String, location: String)(implicit mat: Materializer, attr: Attributes): Future[Bucket] =
    createBucketSource(bucketName, location).withAttributes(attr).runWith(Sink.head)

  def deleteBucketSource(bucketName: String): Source[Done, NotUsed] =
    Source
      .setup { (mat, attr) =>
        implicit val materializer = mat
        implicit val attributes = attr
        import mat.executionContext

        makeRequestSource(
          createRequestSource(HttpMethods.DELETE,
                              uriFactory = (settings: GCStorageSettings) =>
                                Uri(settings.baseUrl).withPath(Path(settings.basePath) ++ getBucketPath(bucketName)))
        ).mapAsync(parallelism) { resp =>
          if (resp.status == StatusCodes.NoContent) {
            Future.successful(Done)
          } else {
            Unmarshal(resp.entity).to[String].flatMap { err =>
              Future.failed(new RuntimeException(s"[${resp.status.intValue}] $err"))
            }
          }
        }

      }
      .mapMaterializedValue(_ => NotUsed)

  def deleteBucket(bucketName: String)(implicit mat: Materializer, attr: Attributes): Future[Done] =
    deleteBucketSource(bucketName).withAttributes(attr).runWith(Sink.head)

  def listBucket(bucket: String, prefix: Option[String], versions: Boolean = false): Source[StorageObject, NotUsed] = {
    sealed trait ListBucketState
    case object Starting extends ListBucketState
    case class Running(nextPageToken: String) extends ListBucketState
    case object Finished extends ListBucketState

    def getBucketListResult(
        pageToken: Option[String]
    )(implicit mat: ActorMaterializer, attr: Attributes): Future[Option[(ListBucketState, List[StorageObject])]] = {
      import mat.executionContext
      val queryParams =
        Map(
          pageToken.map(token => "pageToken" -> token).toList :::
          prefix.map(pref => "prefix" -> pref).toList :::
          (if (versions) List("versions" -> true.toString) else Nil): _*
        )

      makeRequestSource(
        createRequestSource(
          uriFactory = (settings: GCStorageSettings) =>
            Uri(settings.baseUrl)
              .withPath(Path(settings.basePath) ++ getBucketPath(bucket) / "o")
              .withQuery(Query(queryParams))
        )
      ).runWith(Sink.head)
        .flatMap(entityForSuccessOption)
        .flatMap(responseEntityOptionTo[BucketListResult])
        .map { bucketListResultOption =>
          bucketListResultOption.map { result =>
            result.nextPageToken.fold[(ListBucketState, List[StorageObject])]((Finished, result.items))(
              token => (Running(token), result.items)
            )
          }
        }
    }

    Source
      .setup { (mat, attr) =>
        implicit val materializer = mat
        implicit val attributes = attr
        Source
          .unfoldAsync[ListBucketState, List[StorageObject]](Starting) {
            case Finished => Future.successful(None)
            case Starting => getBucketListResult(None)
            case Running(token) => getBucketListResult(Some(token))
          }
          .mapConcat(identity)
      }
      .mapMaterializedValue(_ => NotUsed)
  }

  def getObject(bucket: String,
                objectName: String,
                generation: Option[Long] = None): Source[Option[StorageObject], NotUsed] =
    Source
      .setup { (mat, attr) =>
        implicit val materializer = mat
        implicit val attributes = attr
        makeRequestSource(
          createRequestSource(
            uriFactory = (settings: GCStorageSettings) =>
              Uri(settings.baseUrl)
                .withPath(Path(settings.basePath) ++ getObjectPath(bucket, objectName))
                .withQuery(Query(generation.map("generation" -> _.toString).toMap))
          )
        ).mapAsync(parallelism)(
          response => processGetStorageObjectResponse(response, mat)
        )
      }
      .mapMaterializedValue(_ => NotUsed)

  def deleteObjectSource(bucket: String,
                         objectName: String,
                         generation: Option[Long] = None): Source[Boolean, NotUsed] =
    Source
      .setup { (mat, attr) =>
        implicit val materializer = mat
        implicit val attribures = attr
        makeRequestSource(
          createRequestSource(
            HttpMethods.DELETE,
            uriFactory = (settings: GCStorageSettings) =>
              Uri(settings.baseUrl)
                .withPath(Path(settings.basePath) ++ getObjectPath(bucket, objectName))
                .withQuery(Query(generation.map("generation" -> _.toString).toMap))
          )
        ).mapAsync(parallelism)(entityForSuccessOption)
          .map(option => option.isDefined)
      }
      .mapMaterializedValue(_ => NotUsed)

  def deleteObjectsByPrefixSource(bucket: String, prefix: Option[String]): Source[Boolean, NotUsed] =
    listBucket(bucket, prefix)
      .flatMapConcat(listBucketResult => deleteObjectSource(bucket, listBucketResult.name))

  def putObject(bucket: String,
                objectName: String,
                data: Source[ByteString, _],
                contentType: ContentType): Source[StorageObject, NotUsed] =
    Source
      .setup { (mat, attr) =>
        implicit val materializer = mat
        implicit val attributes = attr
        val queryParams = Map("uploadType" -> "media", "name" -> objectName)
        makeRequestSource(
          createPostSourceRequestSource(
            contentType,
            data,
            uriFactory = (settings: GCStorageSettings) =>
              Uri(settings.baseUrl)
                .withPath(Path("/upload" + settings.basePath) ++ getBucketPath(bucket) / "o")
                .withQuery(Query(queryParams))
          )
        ).mapAsync(parallelism)(response => processPutStorageObjectResponse(response, mat))

      }
      .mapMaterializedValue(_ => NotUsed)

  def download(bucket: String,
               objectName: String,
               generation: Option[Long] = None): Source[Option[Source[ByteString, NotUsed]], NotUsed] =
    Source
      .setup { (mat, attr) =>
        implicit val materializer = mat
        implicit val attributes = attr
        val queryParams = Map("alt" -> "media") ++ generation.map("generation" -> _.toString)
        makeRequestSource(
          createRequestSource(
            uriFactory = (settings: GCStorageSettings) =>
              Uri(settings.baseUrl)
                .withPath(Path(settings.basePath) ++ getObjectPath(bucket, objectName))
                .withQuery(Query(queryParams))
          )
        ).mapAsync(parallelism)(entityForSuccessOption)
          .map {
            _.map(_.withoutSizeLimit.dataBytes.mapMaterializedValue(_ => NotUsed))
          }

      }
      .mapMaterializedValue(_ => NotUsed)

  def resumableUpload(bucket: String,
                      objectName: String,
                      contentType: ContentType,
                      chunkSize: Int = 5 * 1024 * 1024): Sink[ByteString, Future[StorageObject]] =
    chunkAndRequest(bucket, objectName, contentType, chunkSize)
      .toMat(completionSink())(Keep.right)

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

    def rewriteRequest(
        rewriteToken: Option[String]
    )(implicit mat: ActorMaterializer, attr: Attributes): Future[Option[(RewriteState, RewriteResponse)]] = {
      import mat.executionContext
      val queryParams = rewriteToken.map(token => Map("rewriteToken" -> token)).getOrElse(Map())
      makeRequestSource(
        createRequestSource(
          HttpMethods.POST,
          uriFactory = (settings: GCStorageSettings) =>
            Uri(settings.baseUrl)
              .withPath(Path(settings.basePath) ++ sourcePath / "rewriteTo" ++ destinationPath)
              .withQuery(Query(queryParams))
        ).map(response => response.withEntity(response.entity.withoutSizeLimit))
      ).runWith(Sink.head)
        .flatMap(entityForSuccess)
        .flatMap(responseEntityTo[RewriteResponse])
        .map { rewriteResponse =>
          Some(
            rewriteResponse.rewriteToken.fold[(RewriteState, RewriteResponse)]((Finished, rewriteResponse))(
              token => (Running(token), rewriteResponse)
            )
          )
        }
    }

    Source
      .setup { (mat, attr) =>
        implicit val materializer = mat
        implicit val attributes = attr
        Source
          .unfoldAsync[RewriteState, RewriteResponse](Starting) {
            case Finished => Future.successful(None)
            case Starting => rewriteRequest(None)
            case Running(token) => rewriteRequest(Some(token))
          }
      }
      .toMat(Sink.last[RewriteResponse])(Keep.right)
      .mapMaterializedValue(
        _.flatMap(
          _.resource match {
            case Some(resource) => Future.successful(resource)
            case None => Future.failed(new RuntimeException("Storage object is missing"))
          }
        )(ExecutionContexts.sameThreadExecutionContext)
      )
  }

  private def processGetBucketResponse(response: HttpResponse, materializer: Materializer): Future[Option[Bucket]] = {
    implicit val mat = materializer
    import mat.executionContext

    entityForSuccessOption(response).flatMap(responseEntityOptionTo[Bucket])
  }

  private def processCreateBucketResponse(response: HttpResponse, materializer: Materializer): Future[Bucket] = {
    implicit val mat = materializer
    import mat.executionContext

    entityForSuccess(response).flatMap(responseEntityTo[Bucket])
  }

  private def processGetStorageObjectResponse(response: HttpResponse,
                                              materializer: Materializer): Future[Option[StorageObject]] = {
    implicit val mat = materializer
    import mat.executionContext

    entityForSuccessOption(response).flatMap(responseEntityOptionTo[StorageObject])
  }

  private def processPutStorageObjectResponse(response: HttpResponse,
                                              materializer: Materializer): Future[StorageObject] = {
    implicit val mat = materializer
    import mat.executionContext

    entityForSuccess(response).flatMap(responseEntityTo[StorageObject])
  }

  private def makeRequestSource(
      requestSource: Source[HttpRequest, NotUsed]
  )(implicit mat: ActorMaterializer): Source[HttpResponse, NotUsed] = {
    implicit val sys = mat.system
    requestSource.via(GoogleRetry.singleRequestFlow(Http()))
  }

  class RetryableInternalServerError extends Exception

  private def createRequestSource(
      method: HttpMethod = HttpMethods.GET,
      headers: Seq[HttpHeader] = Seq.empty,
      uriFactory: GCStorageSettings => Uri
  )(implicit mat: ActorMaterializer, attr: Attributes): Source[HttpRequest, NotUsed] = {
    implicit val sys = mat.system
    val conf = resolveSettings()
    val uri = uriFactory(conf)
    val request = HttpRequest(method)
      .withUri(uri)
      .withHeaders(headers.toList)

    createAuthenticatedRequestSource(request)
  }

  private def createRequest(
      method: HttpMethod,
      headers: Seq[HttpHeader] = Seq.empty,
      uriFactory: GCStorageSettings => Uri
  )(implicit mat: ActorMaterializer, attr: Attributes): Future[HttpRequest] =
    createRequestSource(method, headers, uriFactory).runWith(Sink.head)

  private def createPostRequestSource(
      contentType: ContentType,
      bytes: ByteString,
      uriFactory: GCStorageSettings => Uri,
      headers: Seq[HttpHeader] = Seq.empty
  )(implicit mat: ActorMaterializer, attr: Attributes): Source[HttpRequest, NotUsed] = {

    implicit val sys = mat.system
    val conf = resolveSettings()

    val uri = uriFactory(conf)
    val request = HttpRequest(HttpMethods.POST)
      .withUri(uri)
      .withEntity(contentType, bytes)
      .withHeaders(headers.toList)

    createAuthenticatedRequestSource(request)
  }

  private def createEmptyPostRequestSource(
      uriFactory: GCStorageSettings => Uri,
      headers: Seq[HttpHeader]
  )(implicit mat: ActorMaterializer, attr: Attributes): Source[HttpRequest, NotUsed] = {

    implicit val sys = mat.system
    val conf = resolveSettings()

    val uri = uriFactory(conf)
    val request = HttpRequest(HttpMethods.POST)
      .withUri(uri)
      .withHeaders(headers.toList)

    createAuthenticatedRequestSource(request)
  }

  private def createPostSourceRequestSource(
      contentType: ContentType,
      data: Source[ByteString, _],
      uriFactory: GCStorageSettings => Uri,
      headers: Seq[HttpHeader] = Seq.empty
  )(implicit mat: ActorMaterializer, attr: Attributes): Source[HttpRequest, NotUsed] = {

    implicit val sys = mat.system
    val conf = resolveSettings()

    val uri = uriFactory(conf)
    val request = HttpRequest(HttpMethods.POST)
      .withUri(uri)
      .withEntity(HttpEntity(contentType, data))
      .withHeaders(headers.toList)

    createAuthenticatedRequestSource(request)
  }

  private def createAuthenticatedRequestSource(request: HttpRequest)(implicit mat: ActorMaterializer,
                                                                     attr: Attributes): Source[HttpRequest, NotUsed] = {
    import mat.executionContext
    implicit val sys = mat.system
    val conf = resolveSettings()
    val http = Http()(mat.system)
    val session: GoogleSession =
      new GoogleSession(conf.clientEmail,
                        conf.privateKey,
                        new GoogleTokenApi(http, TokenApiSettings(conf.tokenUrl, conf.tokenScope)))

    Source.fromFuture(session.getToken().map { accessToken =>
      request.addCredentials(OAuth2BearerToken(accessToken))
    })
  }

  private def getBucketPath(bucket: String) =
    Path("/b") / bucket

  private def getObjectPath(bucket: String, objectName: String) =
    getBucketPath(bucket) / "o" / objectName

  private def swap[A](option: Option[Future[A]])(implicit ec: ExecutionContext): Future[Option[A]] =
    option match {
      case Some(f) => f.map(Some(_))
      case None => Future.successful(None)
    }

  private def responseEntityOptionTo[A](
      responseOption: Option[ResponseEntity]
  )(implicit aRead: JsonReader[A], mat: Materializer): Future[Option[A]] = {
    import mat.executionContext
    swap(responseOption.map(resp => responseEntityTo(resp)(aRead, mat)))
  }

  private def responseEntityTo[A](response: ResponseEntity)(implicit aRead: JsonReader[A],
                                                            mat: Materializer): Future[A] = {
    import mat.executionContext
    Unmarshal(response).to[JsValue].map(data => data.convertTo[A])
  }

  private def entityForSuccessOption(
      resp: HttpResponse
  )(implicit mat: Materializer): Future[Option[ResponseEntity]] = {

    import mat.executionContext

    resp match {
      case HttpResponse(status, _, entity, _) if status.isSuccess() && !status.isRedirection() =>
        Future.successful(Some(entity))
      case HttpResponse(StatusCodes.NotFound, _, entity, _) =>
        entity.discardBytes()
        Future.successful(None)
      case HttpResponse(status, _, entity, _) =>
        Unmarshal(entity).to[String].flatMap { err =>
          Future.failed(new RuntimeException(s"[${status.intValue}] $err"))
        }
    }
  }

  private def entityForSuccess(resp: HttpResponse)(implicit mat: Materializer): Future[ResponseEntity] = {
    import mat.executionContext

    resp match {
      case HttpResponse(status, _, entity, _) if status.isSuccess() && !status.isRedirection() =>
        Future.successful(entity)
      case HttpResponse(status, _, entity, _) =>
        Unmarshal(entity).to[String].flatMap { err =>
          Future.failed(new RuntimeException(s"[${status.intValue}] $err"))
        }
    }
  }

  private def chunkAndRequest(bucket: String,
                              objectName: String,
                              contentType: ContentType,
                              chunkSize: Int): Flow[ByteString, UploadPartResponse, NotUsed] =
    // The individual upload part requests are processed here
    // apparently Google Cloud storage does not support parallel uploading
    Flow
      .setup { (mat, _) =>
        implicit val materializer = mat
        implicit val sys = mat.system
        import mat.executionContext
        // Multipart upload requests are created here.
        //  The initial upload request gets executed within this function as well.
        //  The individual upload part requests are created.

        createRequests(bucket, objectName, contentType, chunkSize)
          .mapAsync(parallelism) {
            case (req, (upload, index)) =>
              GoogleRetry
                .singleRequest(Http(), req)
                .map(resp => (Success(resp), (upload, index)))
                .recoverWith {
                  case NonFatal(e) => Future.successful((Failure(e), (upload, index)))
                }
          }
          .mapAsync(parallelism) {
            // 308 Resume incomplete means that chunk was successfully transfered but more chunks are expected
            case (Success(HttpResponse(StatusCodes.PermanentRedirect, _, entity, _)), (upload, index)) =>
              entity.discardBytes()
              Future.successful(SuccessfulUploadPart(upload, index))
            case (Success(HttpResponse(status, _, entity, _)), (upload, index))
                if status.isSuccess() && !status.isRedirection() =>
              responseEntityTo[StorageObject](entity).map(so => SuccessfulUpload(upload, index, so))
            case (Success(HttpResponse(status, _, entity, _)), (upload, index)) =>
              Unmarshal(entity)
                .to[String]
                .map(
                  errorString =>
                    FailedUploadPart(upload,
                                     index,
                                     new RuntimeException(s"Uploading part failed with status $status: $errorString"))
                )
            case (Failure(e), (upload, index)) =>
              Future.successful(FailedUploadPart(upload, index, e))
          }
      }
      .mapMaterializedValue(_ => NotUsed)

  private def createRequests(bucketName: String,
                             objectName: String,
                             contentType: ContentType,
                             chunkSize: Int): Flow[ByteString, (HttpRequest, (MultiPartUpload, Int)), NotUsed] = {
    // First step of the resumable upload process is made.
    //  The response is then used to construct the subsequent individual upload part requests
    val requestInfo: Source[(MultiPartUpload, Int), NotUsed] = initiateUpload(bucketName, objectName, contentType)

    Flow
      .setup { (mat, attr) =>
        implicit val materializer = mat
        implicit val attributes = attr
        import materializer.executionContext
        Flow
          .apply[ByteString]
          .via(new Chunker(chunkSize))
          .zipWith(requestInfo) {
            case (chunk, info) => (chunk, info)
          }
          .mapAsync(parallelism) {
            case (chunkedPayload, (uploadInfo, chunkIndex)) =>
              val queryParams =
                Map("uploadType" -> "resumable", "name" -> objectName, "upload_id" -> uploadInfo.uploadId)
              createRequest(
                HttpMethods.PUT,
                uriFactory = (settings: GCStorageSettings) =>
                  Uri(settings.baseUrl)
                    .withPath(Path("/upload" + settings.basePath) ++ getBucketPath(bucketName) / "o")
                    .withQuery(Query(queryParams))
              ).map { req =>
                  // add payload and Content-Range header
                  req
                    .withEntity(
                      HttpEntity(ContentTypes.`application/octet-stream`,
                                 chunkedPayload.size,
                                 Source.single(chunkedPayload.bytes))
                    )
                    // make sure we do these calculations in the Long range !!!! We talk about potentially huge files
                    // Int is limited to 2,parallelismGb !!
                    .addHeader(
                      `Content-Range`(
                        ContentRange((chunkIndex - 1L) * chunkSize,
                                     ((chunkIndex - 1L) * chunkSize) + Math.max(chunkedPayload.size, 1L) - 1,
                                     chunkedPayload.totalSize)
                      )
                    )
                }
                .map((_, (uploadInfo, chunkIndex)))
          }
      }
      .mapMaterializedValue(_ => NotUsed)
  }

  private def initiateUpload(bucketName: String,
                             objectName: String,
                             contentType: ContentType): Source[(MultiPartUpload, Int), NotUsed] =
    Source
      .setup { (mat, attr) =>
        implicit val materializer = mat
        implicit val attributes = attr
        import mat.executionContext
        val queryParams = Map("uploadType" -> "resumable", "name" -> objectName)
        makeRequestSource(
          createEmptyPostRequestSource(
            uriFactory = (settings: GCStorageSettings) =>
              Uri(settings.baseUrl)
                .withPath(Path("/upload" + settings.basePath) ++ getBucketPath(bucketName) / "o")
                .withQuery(Query(queryParams)),
            Seq(RawHeader("X-Upload-Content-Type", contentType.toString()))
          )
        ).mapAsync(parallelism) {
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
            case HttpResponse(status, _, entity, _) =>
              Unmarshal(entity).to[String].flatMap { err =>
                Future.failed(new RuntimeException(s"[${status.intValue}] $err"))
              }
          }
          .mapConcat(r => Stream.continually(r))
          .zip(Source.fromIterator(() => Iterator.from(parallelism)))
      }
      .mapMaterializedValue(_ => NotUsed)

  private def completionSink(): Sink[UploadPartResponse, Future[StorageObject]] =
    Sink
      .setup { (mat, _) =>
        import mat.executionContext

        Sink.seq[UploadPartResponse].mapMaterializedValue { responseFuture: Future[Seq[UploadPartResponse]] =>
          responseFuture
            .flatMap { responses: Seq[UploadPartResponse] =>
              //val successes = responses.collect { case r: SuccessfulUploadPart => r }
              val storageObjectResult = responses.collect { case so: SuccessfulUpload => so }
              val failures = responses.collect { case r: FailedUploadPart => r }.toList
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
      .mapMaterializedValue(_.flatMap(identity)(ExecutionContexts.sameThreadExecutionContext))

  private def resolveSettings()(implicit attr: Attributes, sys: ActorSystem) =
    attr
      .get[GCStorageSettingsValue]
      .map(_.settings)
      .getOrElse {
        val configPath = attr.get[GCStorageSettingsPath](GCStorageSettingsPath.Default).path
        GCStorageExt(sys).settings(configPath)
      }
}
