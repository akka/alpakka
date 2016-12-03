/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ironmq

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.HostConnectionPool
import akka.http.scaladsl.client.RequestBuilding._
import akka.http.scaladsl.model.headers.{ Authorization, GenericHttpCredentials }
import akka.http.scaladsl.model.{ HttpRequest, HttpResponse, Uri }
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.http.scaladsl.util.FastFuture
import akka.stream.{ ActorMaterializer, Materializer }
import akka.stream.scaladsl.{ Flow, Sink, Source }
import akka.{ Done, NotUsed }
import com.typesafe.config.Config
import de.heikoseeberger.akkahttpcirce.CirceSupport._
import io.circe.Json
import io.circe.syntax._
import cats.syntax.either._

import scala.concurrent.duration.{ Duration, FiniteDuration }
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

object IronMqClient {

  def apply()(implicit actorSystem: ActorSystem, materializer: Materializer): IronMqClient =
    apply(actorSystem.settings.config)

  def apply(config: Config)(implicit actorSystem: ActorSystem, materializer: Materializer): IronMqClient =
    apply(IronMqSettings(config))

  def apply(settings: IronMqSettings)(implicit actorSystem: ActorSystem, materializer: Materializer): IronMqClient =
    new IronMqClient(settings)

}

class IronMqClient(settings: IronMqSettings)(implicit actorSystem: ActorSystem, materializer: Materializer) {

  import Codec._

  private val http = Http(actorSystem)

  private val connectionPoolFlow: Flow[(HttpRequest, NotUsed), (Try[HttpResponse], NotUsed), HostConnectionPool] = {
    val endpoint = settings.endpoint
    endpoint.scheme match {
      case "https" =>
        http.cachedHostConnectionPoolHttps[NotUsed](endpoint.authority.host.address(), endpoint.authority.port)
      case "http" => http.cachedHostConnectionPool[NotUsed](endpoint.authority.host.address(), endpoint.authority.port)
      case other =>
        throw new IllegalArgumentException(s"Endpoint $endpoint contains an invalid HTTP/HTTPS scheme $other")
    }
  }

  private val pipeline: Flow[HttpRequest, HttpResponse, _] = Flow[HttpRequest]
    .map(_.withHeaders(Authorization(GenericHttpCredentials("OAuth", settings.token))))
    .map(_ -> NotUsed)
    .via(connectionPoolFlow)
    .map(_._1)
    .mapAsync(1) {
      case Success(response) if response.status.isSuccess() =>
        FastFuture.successful(response)
      case Success(response) if !response.status.isSuccess() =>
        FastFuture.failed(new RuntimeException(response.status.reason()))
      case Failure(error) =>
        FastFuture.failed(error)
    }

  def listQueues(prefix: Option[String] = None, from: Option[Queue.Name] = None, noOfQueues: Int = 50)(
      implicit ec: ExecutionContext): Future[Traversable[Queue.Name]] = {

    def parseQueues(json: Json) = {

      def extractName(json: Json) = json.hcursor.downField("name").as[Json].getOrElse(Json.Null)

      json.hcursor
        .downField("queues")
        .withFocus { xsJson =>
          xsJson.mapArray { xs =>
            xs.map(extractName)
          }
        }
        .as[Traversable[Queue.Name]]
    }

    val query = List(prefix.map("prefix" -> _), from.map("previous" -> _.value)).collect {
      case Some(x) => x
    }.foldLeft(Uri.Query("per_page" -> noOfQueues.toString)) { (q, x) =>
      x +: q
    }

    makeRequest(Get(Uri(s"$queuesPath").withQuery(query))).flatMap(Unmarshal(_).to[Json]).map(parseQueues).collect {
      case Right(xs) => xs
    }
  }

  def createQueue(name: Queue.Name)(implicit ec: ExecutionContext): Future[Queue] =
    makeRequest(Put(Uri(s"$queuesPath/${name.value}"), Json.obj()))
      .flatMap(Unmarshal(_).to[Json])
      .map(_.hcursor.downField("queue").as[Queue])
      .collect {
        case Right(queue) => queue
      }

  def deleteQueue(name: Queue.Name)(implicit ec: ExecutionContext): Future[Done] =
    makeRequest(Delete(Uri(s"$queuesPath/${name.value}"))).map(_ => Done)

  def pushMessages(queueName: Queue.Name, messages: PushMessage*)(implicit ec: ExecutionContext): Future[Message.Ids] = {

    val payload = Json.obj(
        "messages" -> Json.fromValues(
          messages.map { pm =>
          Json.obj("body" -> Json.fromString(pm.body), "delay" -> Json.fromLong(pm.delay.toSeconds))
        }
        ))

    makeRequest(Post(Uri(s"$queuesPath/${queueName.value}/messages"), payload)).flatMap(Unmarshal(_).to[Message.Ids])
  }

  def reserveMessages(
      queueName: Queue.Name,
      noOfMessages: Int = 1,
      timeout: Duration = Duration.Undefined,
      watch: Duration = Duration.Undefined)(implicit ec: ExecutionContext): Future[Iterable[ReservedMessage]] = {

    val payload = (if (timeout.isFinite()) {
                   Json.obj("timeout" -> Json.fromLong(timeout.toSeconds))
                 } else {
                   Json.Null
                 }) deepMerge (if (watch.isFinite()) {
                               Json.obj("wait" -> Json.fromLong(watch.toSeconds))
                             } else {
                               Json.Null
                             }) deepMerge Json.obj("n" -> Json.fromInt(noOfMessages),
        "delete" -> Json.fromBoolean(false))

    makeRequest(Post(Uri(s"$queuesPath/${queueName.value}/reservations"), payload))
      .flatMap(Unmarshal(_).to[Json])
      .map { json =>
        json.hcursor.downField("messages").as[Iterable[ReservedMessage]]
      }
      .collect {
        case Right(xs) => xs
      }
  }

  def pullMessages(queueName: Queue.Name,
                   noOfMessages: Int = 1,
                   timeout: Duration = Duration.Undefined,
                   watch: Duration = Duration.Undefined)(implicit ec: ExecutionContext): Future[Iterable[Message]] = {

    val payload = (if (timeout.isFinite()) {
                   Json.obj("timeout" -> Json.fromLong(timeout.toSeconds))
                 } else {
                   Json.Null
                 }) deepMerge (if (watch.isFinite()) {
                               Json.obj("wait" -> Json.fromLong(watch.toSeconds))
                             } else {
                               Json.Null
                             }) deepMerge Json.obj("n" -> Json.fromInt(noOfMessages),
        "delete" -> Json.fromBoolean(true))

    makeRequest(Post(Uri(s"$queuesPath/${queueName.value}/reservations"), payload))
      .flatMap(Unmarshal(_).to[Json])
      .map { json =>
        json.hcursor.downField("messages").as[Iterable[Message]]
      }
      .collect {
        case Right(xs) => xs
      }
  }

  def touchMessage(queueName: Queue.Name, reservation: Reservation, timeout: Duration = Duration.Undefined)(
      implicit ec: ExecutionContext): Future[Reservation] = {

    val payload = (if (timeout.isFinite()) {
                   Json.obj("timeout" -> timeout.toSeconds.asJson)
                 } else {
                   Json.Null
                 }) deepMerge Json.obj("reservation_id" -> reservation.reservationId.asJson)

    makeRequest(Post(s"$queuesPath/${queueName.value}/messages/${reservation.messageId}/touch", payload))
      .flatMap(Unmarshal(_).to[Json])
      .map { json =>
        for {
          reservationId <- json.hcursor.downField("reservation_id").as[Reservation.Id]
        } yield reservation.copy(reservationId = reservationId)
      }
      .collect {
        case Right(r) => r
      }

  }

  def peekMessages(queueName: Queue.Name, numberOfMessages: Int = 1)(
      implicit ec: ExecutionContext): Future[Iterable[Message]] =
    makeRequest(
        Get(Uri(s"$queuesPath/${queueName.value}/messages").withQuery(Uri.Query("n" -> numberOfMessages.toString))))
      .flatMap(Unmarshal(_).to[Json])
      .map { json =>
        json.hcursor.downField("messages").as[Iterable[Message]]
      }
      .collect {
        case Right(xs) => xs
      }

  def deleteMessage(queueName: Queue.Name, reservation: Reservation)(implicit ec: ExecutionContext): Future[Unit] = {

    val payload = reservation.asJson

    makeRequest(Delete(Uri(s"$queuesPath/${queueName.value}/messages/${reservation.messageId}"), payload))
      .map(_ => Unit)

  }

  def deleteMessages(queueName: Queue.Name, reservations: Iterable[Reservation])(
      implicit ec: ExecutionContext): Future[Unit] = {

    val payload = Json.obj("ids" -> Json.fromValues(reservations.map(_.asJson).toSeq))

    makeRequest(Delete(Uri(s"$queuesPath/${queueName.value}/messages"), payload)).map(_ => Unit)

  }

  def releaseMessage(queueName: Queue.Name, reservation: Reservation, delay: FiniteDuration = Duration.Zero)(
      implicit ec: ExecutionContext): Future[Unit] = {

    val payload = Json.obj("reservation_id" -> reservation.reservationId.asJson, "delay" -> delay.toSeconds.asJson)

    makeRequest(Post(Uri(s"$queuesPath/${queueName.value}/messages/${reservation.messageId.value}/release"), payload))
      .map(_ => Unit)
  }

  def clearMessages(queue: String)(implicit ec: ExecutionContext): Future[Unit] =
    makeRequest(Delete(Uri(s"$queuesPath/$queue/messages"), Json.obj())).map(_ => Unit)

  private val queuesPath = s"/3/projects/${settings.projectId}/queues"

  private def makeRequest(request: HttpRequest): Future[HttpResponse] =
    Source.single(request).via(pipeline).runWith(Sink.head)

}
