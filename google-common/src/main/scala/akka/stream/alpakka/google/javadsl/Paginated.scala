/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.javadsl

import akka.stream.alpakka.google.scaladsl

import java.util
import scala.jdk.OptionConverters._

/**
 * Models a paginated resource
 */
trait Paginated {

  /**
   * Returns the token for the next page, if present
   */
  def getPageToken: util.Optional[String]
}

private[alpakka] object Paginated {
  implicit object paginatedIsPaginated extends scaladsl.Paginated[Paginated] {
    override def pageToken(paginated: Paginated): Option[String] = paginated.getPageToken.toScala
  }
}
