/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.alpakka.googlecloud.bigquery.BigQueryConfig
import akka.stream.alpakka.googlecloud.bigquery.scaladsl.{BigQueryCallbacks, GoogleBigQuerySource}
import akka.stream.scaladsl.Source

import scala.concurrent.Future

// Separate class to circumvent ambiguous implicits for unmarshalling
class GoogleBigQuerySourceCustomParserDoc {

  import akka.stream.alpakka.googlecloud.bigquery.client.ResponseJsonProtocol

  implicit val system = ActorSystem()
  val config = BigQueryConfig("project@test.test", "privateKeyFromGoogle", "projectID", "bigQueryDatasetName")

  //#custom-parser
  import akka.http.scaladsl.unmarshalling.{FromByteStringUnmarshaller, Unmarshaller}
  import io.circe.{jawn, Decoder, Json}
  import io.circe.generic.semiauto

  implicit val jsonUnmarshaller: FromByteStringUnmarshaller[Json] = Unmarshaller { _ => byteString =>
    Future.fromTry(jawn.parseByteBuffer(byteString.asByteBuffer).toTry)
  }

  implicit def fromJsonUnmarshaller[T](implicit decoder: Decoder[T]): Unmarshaller[Json, T] = Unmarshaller {
    _ => json =>
      Future.fromTry(decoder.decodeJson(json).toTry)
  }

  case class User(uid: String, name: String)

  implicit val userDecoder: Decoder[User] = Decoder.instance { c =>
    val f = c.downField("f")
    for {
      uid <- f.downN(0).downField("v").as[String]
      name <- f.downN(1).downField("v").as[String]
    } yield User(uid, name)
  }

  implicit val jobReferenceDecoder: Decoder[ResponseJsonProtocol.JobReference] = semiauto.deriveDecoder
  implicit val responseDecoder: Decoder[ResponseJsonProtocol.Response] = semiauto.deriveDecoder

  implicit def rowsDecoder[T](implicit decoder: Decoder[T]): Decoder[ResponseJsonProtocol.ResponseRows[T]] =
    semiauto.deriveDecoder

  val userStream: Source[User, NotUsed] =
    GoogleBigQuerySource[User]
      .runQuery("SELECT uid, name FROM bigQueryDatasetName.myTable", BigQueryCallbacks.ignore, config)
  //#custom-parser
}
