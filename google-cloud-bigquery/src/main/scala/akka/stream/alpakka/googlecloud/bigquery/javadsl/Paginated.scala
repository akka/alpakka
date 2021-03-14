/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.javadsl

import akka.stream.alpakka.googlecloud.bigquery.scaladsl

import java.util
import scala.compat.java8.OptionConverters._

/**
 * Models a paginated resource
 */
trait Paginated {

  /**
   * Returns the token for the next page, if present
   */
  def getPageToken: util.Optional[String]
}

private[javadsl] object Paginated {
  implicit object paginatedIsPaginated extends scaladsl.Paginated[Paginated] {
    override def pageToken(paginated: Paginated): Option[String] = paginated.getPageToken.asScala
  }
}
