/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.logging.model

import akka.annotation.InternalApi
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.Uri.{Authority, Path}
import spray.json.{deserializationError, JsString, JsValue, JsonFormat}

import scala.annotation.tailrec
import scala.util.Try

@InternalApi
private[model] object PathJsonProtocol {

  /**
   * Akka HTTP represents [[Path]] as [[String]] in Java DSL.
   */
  implicit final class PathAsString(val path: Path) extends AnyVal {
    def asString: String = pathAsString(path)
  }

  /**
   * Renders a path as a string without URI encoding.
   */
  @tailrec
  private def pathAsString(path: Path, sb: StringBuilder = StringBuilder.newBuilder): String = path match {
    case Path.Empty => sb.toString
    case Path.Slash(tail) => pathAsString(tail, sb += '/')
    case Path.Segment(head, tail) => pathAsString(tail, sb ++= head)
  }

  implicit object pathFormat extends JsonFormat[Path] {
    override def write(obj: Path): JsValue = JsString(obj.toString)
    override def read(json: JsValue): Path = json match {
      case JsString(EncodedPath(path)) => path
      case x => deserializationError("Expected path as JsString, but got " + x)
    }
  }

  private object EncodedPath {
    def unapply(path: String): Option[Path] =
      Try(Uri(path)).toOption.flatMap {
        case Uri("", Authority.Empty, path, None, None) => Some(path)
        case _ => None
      }
  }
}
